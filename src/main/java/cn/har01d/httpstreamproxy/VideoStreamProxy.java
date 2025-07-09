package cn.har01d.httpstreamproxy;

import fi.iki.elonen.NanoHTTPD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.*;
import java.util.concurrent.*;

public class VideoStreamProxy extends NanoHTTPD {
    private static final Logger log = LoggerFactory.getLogger(VideoStreamProxy.class);

    public static class Chunk {
        final int id;
        final long start;
        final long end;
        byte[] data;

        public Chunk(int id, long start, long end) {
            this.id = id;
            this.start = start;
            this.end = end;
        }
    }

    public static class Session {
        final String videoUrl;
        final long startOffset;
        final long rangeEnd;
        final long chunkSize;
        final int threadCount;

        final BlockingQueue<Chunk> queue;
        final ExecutorService executor;
        volatile boolean running = true;
        volatile int chunkId = 0;

        public Session(String videoUrl, long startOffset, long rangeEnd, int threadCount, long chunkSize) {
            this.videoUrl = videoUrl;
            this.startOffset = startOffset;
            this.rangeEnd = rangeEnd;
            this.threadCount = threadCount;
            this.chunkSize = chunkSize;
            this.queue = new LinkedBlockingQueue<>(threadCount);
            this.executor = Executors.newFixedThreadPool(threadCount);
        }
    }

    public VideoStreamProxy() throws IOException {
        super(9000);
        start(SOCKET_READ_TIMEOUT, false);
        log.info("Proxy started at http://localhost:9000");
    }

    @Override
    public Response serve(IHTTPSession httpSession) {
        String uri = httpSession.getUri();
        if (!uri.startsWith("/proxy/")) {
            return newFixedLengthResponse(Response.Status.NOT_FOUND, MIME_PLAINTEXT, "Not found");
        }

        String id = uri.substring("/proxy/".length());
        int threads = 8;
        int chunkSize = 512 * 1024;
        //String videoUrl = "https://d.har01d.cn/AI_News.mp4";
        String videoUrl = "http://10.121.235.6/test/video/spring_boot.mp4";

        try {
            Map<String, String> headers = getOriginalHeaders(videoUrl);
            long totalLength = Long.parseLong(headers.get("Content-Length"));
            String contentType = headers.getOrDefault("Content-Type", "video/mp4");

            String rangeHeader = httpSession.getHeaders().getOrDefault("range", "bytes=0-");
            long rangeStart = parseRangeStart(rangeHeader);
            long rangeEnd = parseRangeEnd(rangeHeader, totalLength - 1);
            long contentLength = rangeEnd - rangeStart + 1;

            log.info("GET {} Range: {} -> {} totalLength={}", uri, rangeStart, rangeEnd, totalLength);

            long quickStartSize = 64 * 1024;
            long firstChunkEnd = Math.min(rangeStart + quickStartSize - 1, rangeEnd);
            Chunk firstChunk = new Chunk(-1, rangeStart, firstChunkEnd);
            downloadChunk(videoUrl, firstChunk);
            log.info("Download chunk -1 [{}-{}]", firstChunk.start, firstChunk.end);

            PipedInputStream inPipe = new PipedInputStream(8 * 1024 * 1024);
            PipedOutputStream outPipe = new PipedOutputStream(inPipe);

            Response response = newFixedLengthResponse(Response.Status.PARTIAL_CONTENT, contentType, inPipe, contentLength);
            response.addHeader("Content-Range", "bytes " + rangeStart + "-" + rangeEnd + "/" + totalLength);
            response.addHeader("Content-Length", String.valueOf(contentLength));
            response.addHeader("Accept-Ranges", "bytes");
            response.addHeader("Access-Control-Allow-Origin", "*");
            //response.addHeader("Connection", "close");
            copyResponseHeaders(response, headers);

            log.info("write first chunk to {}", firstChunk.end);
            outPipe.write(firstChunk.data);
            outPipe.flush();
            log.info("first chunk sent");

            if (firstChunk.end < rangeEnd) {
                Session session = new Session(videoUrl, firstChunkEnd + 1, rangeEnd, threads, chunkSize);
                for (int i = 0; i < threads; i++) {
                    startWorker(session);
                }

                new Thread(() -> {
                    try (outPipe) {
                        int expectedId = 0;
                        Map<Integer, Chunk> buffer = new TreeMap<>();

                        while (session.running) {
                            Chunk chunk = session.queue.poll(100, TimeUnit.MILLISECONDS);
                            if (chunk != null) {
                                buffer.put(chunk.id, chunk);
                            }

                            while (buffer.containsKey(expectedId)) {
                                Chunk c = buffer.remove(expectedId++);
                                log.info("write chunk: {}", c.id);
                                outPipe.write(c.data);
                                outPipe.flush();
                                log.info("chunk {} sent", c.id);
                                if (c.end >= rangeEnd) {
                                    session.running = false;
                                    break;
                                }
                            }
                        }
                    } catch (IOException | InterruptedException e) {
                        log.warn("Writer error: {}", e.toString());
                    } finally {
                        session.running = false;
                        session.executor.shutdownNow();
                    }
                }, "writer").start();
            } else {
                outPipe.close();
            }

            return response;
        } catch (Exception e) {
            log.error("serve failed", e);
            return newFixedLengthResponse(Response.Status.INTERNAL_ERROR, MIME_PLAINTEXT, "Internal error");
        }
    }

    private void startWorker(Session session) {
        session.executor.submit(() -> {
            log.info("start worker thread");
            while (session.running) {
                int cid;
                synchronized (session) {
                    cid = session.chunkId++;
                }
                long start = session.startOffset + (cid * session.chunkSize);
                if (start > session.rangeEnd) {
                    break;
                }
                long end = Math.min(start + session.chunkSize - 1, session.rangeEnd);

                Chunk chunk = new Chunk(cid, start, end);
                try {
                    downloadChunk(session.videoUrl, chunk);
                    session.queue.put(chunk);
                } catch (Exception e) {
                    log.warn("Worker failed cid {}: {}", cid, e.toString());
                    break;
                }
            }
        });
    }

    private static void downloadChunk(String urlStr, Chunk chunk) throws IOException {
        int retries = 3;
        while (retries-- > 0) {
            try {
                HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection();
                conn.setRequestProperty("Range", "bytes=" + chunk.start + "-" + chunk.end);
                conn.connect();

                int size = (int) (chunk.end - chunk.start + 1);
                ByteBuffer buffer = ByteBuffer.allocate(size);
                try (ReadableByteChannel channel = Channels.newChannel(conn.getInputStream())) {
                    while (buffer.hasRemaining() && channel.read(buffer) > 0);
                }
                buffer.flip();
                chunk.data = Arrays.copyOf(buffer.array(), buffer.limit());
                log.info("downloaded chunk {}", chunk.id);
                return;
            } catch (IOException e) {
                log.warn("Retry download chunk {} due to {}", chunk.id, e.toString());
            }
        }
        throw new IOException("Download failed after retries for chunk " + chunk.id);
    }

    private Map<String, String> getOriginalHeaders(String videoUrl) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(videoUrl).openConnection();
        conn.setRequestMethod("HEAD");
        conn.setConnectTimeout(3000);
        conn.setReadTimeout(3000);
        conn.connect();

        Map<String, String> headers = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> entry : conn.getHeaderFields().entrySet()) {
            if (entry.getKey() != null && !entry.getValue().isEmpty()) {
                headers.put(entry.getKey(), entry.getValue().get(0));
            }
        }
        headers.put("Content-Length", String.valueOf(conn.getContentLengthLong()));
        return headers;
    }

    private static long parseRangeStart(String rangeHeader) {
        if (rangeHeader == null || !rangeHeader.startsWith("bytes=")) return 0;
        String[] parts = rangeHeader.substring(6).split("-");
        return Long.parseLong(parts[0]);
    }

    private static long parseRangeEnd(String rangeHeader, long defaultEnd) {
        if (rangeHeader == null || !rangeHeader.startsWith("bytes=")) return defaultEnd;
        String[] parts = rangeHeader.substring(6).split("-");
        if (parts.length >= 2 && !parts[1].isEmpty()) {
            return Long.parseLong(parts[1]);
        }
        return defaultEnd;
    }

    private static void copyResponseHeaders(Response resp, Map<String, String> headers) {
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            String key = entry.getKey();
            if (key == null) continue;
            if (key.equalsIgnoreCase("Content-Length")
                    || key.equalsIgnoreCase("Content-Type")
                    || key.equalsIgnoreCase("Content-Range")) {
                continue;
            }
            resp.addHeader(key, entry.getValue());
        }
    }

    public static void main(String[] args) throws IOException {
        new VideoStreamProxy();
    }
}
