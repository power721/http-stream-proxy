package cn.har01d.httpstreamproxy;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fi.iki.elonen.NanoHTTPD;

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
        final long totalLength;
        final long chunkSize;
        final int threadCount;

        final BlockingQueue<Chunk> queue;
        final ExecutorService executor;
        volatile boolean running = true;
        volatile int chunkId = 0;

        public Session(String videoUrl, long startOffset, long totalLength, int threadCount, long chunkSize) {
            this.videoUrl = videoUrl;
            this.startOffset = startOffset;
            this.totalLength = totalLength;
            this.threadCount = threadCount;
            this.chunkSize = chunkSize;
            this.queue = new LinkedBlockingQueue<>(threadCount);
            this.executor = Executors.newFixedThreadPool(threadCount);
        }
    }

    public VideoStreamProxy() throws IOException {
        super(9000);
        start(NanoHTTPD.SOCKET_READ_TIMEOUT, false);
        System.out.println("Proxy started at http://localhost:9000");
    }

    @Override
    public Response serve(IHTTPSession session) {
        String uri = session.getUri();
        if (!uri.startsWith("/proxy/")) {
            return newFixedLengthResponse(Response.Status.NOT_FOUND, MIME_PLAINTEXT, "Not found");
        }

        log.info("{} {}", session.getMethod(), session.getUri());

        String id = uri.substring("/proxy/".length());
        String videoUrl = "http://10.121.235.6/test/video/spring_boot.mp4";

        try {
            Map<String, String> headers = getOriginalHeaders(videoUrl);
            long totalLength = Long.parseLong(headers.get("Content-Length"));
            String contentType = headers.getOrDefault("Content-Type", "video/mp4");

            if (session.getHeaders().containsKey("range")) {
                Response response = newFixedLengthResponse(Response.Status.OK, contentType, "");
                copyResponseHeaders(response, headers);
                return response;
            }

            String rangeHeader = session.getHeaders().getOrDefault("range", "bytes=0-");
            long rangeStart = parseRangeStart(rangeHeader);
            long rangeEnd = totalLength - 1;
            long contentLength = rangeEnd - rangeStart + 1;

            int chunkSize = 512 * 1024;
            PipedInputStream inPipe = new PipedInputStream(chunkSize);
            PipedOutputStream outPipe = new PipedOutputStream(inPipe);

            Response response = newFixedLengthResponse(Response.Status.PARTIAL_CONTENT, contentType, inPipe, contentLength);
            copyResponseHeaders(response, headers);

            long start = rangeStart;
            if (session.getMethod() == Method.GET) {
                long quickStartSize = 256 * 1024;
                long firstChunkEnd = Math.min(rangeStart + quickStartSize - 1, rangeEnd);
                Chunk firstChunk = new Chunk(-1, rangeStart, firstChunkEnd);
                downloadChunk(videoUrl, firstChunk);

                log.info("write first chunk to {}", firstChunkEnd);
                outPipe.write(firstChunk.data);
                outPipe.flush();
                start = firstChunkEnd;
                log.info("first chunk sent");
            }

            Session vidSession = new Session(videoUrl, start, totalLength, 4, chunkSize);
            for (int i = 0; i < vidSession.threadCount; i++) {
                startWorker(vidSession);
            }

            new Thread(() -> {
                try (outPipe) {
                    int expectedId = 0;
                    Map<Integer, Chunk> buffer = new TreeMap<>();

                    while (vidSession.running) {
                        Chunk chunk = vidSession.queue.poll(100, TimeUnit.MILLISECONDS);
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
                                vidSession.running = false;
                                break;
                            }
                        }
                    }
                } catch (IOException | InterruptedException e) {
                    log.warn("Stream interrupted", e);
                } finally {
                    vidSession.executor.shutdownNow();
                }
            }, "writer").start();

            return response;

        } catch (Exception e) {
            log.error("serve failed", e);
            return newFixedLengthResponse(Response.Status.INTERNAL_ERROR, MIME_PLAINTEXT, "Internal error");
        }
    }

    private void startWorker(Session session) {
        session.executor.submit(() -> {
            while (session.running) {
                int cid;
                long start, end;
                synchronized (session) {
                    cid = session.chunkId++;
                    start = session.startOffset + cid * session.chunkSize;
                    if (start >= session.totalLength) break;
                    end = Math.min(start + session.chunkSize - 1, session.totalLength - 1);
                }
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
        log.info("Download chunk {} [{}-{}]", chunk.id, chunk.start, chunk.end);
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestProperty("Range", "bytes=" + chunk.start + "-" + chunk.end);
        conn.connect();

        int size = (int) (chunk.end - chunk.start + 1);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        try (ReadableByteChannel channel = Channels.newChannel(conn.getInputStream())) {
            while (buffer.hasRemaining() && channel.read(buffer) > 0) ;
        }
        buffer.flip();
        chunk.data = Arrays.copyOf(buffer.array(), buffer.limit());
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

    private static void copyResponseHeaders(Response resp, Map<String, String> headers) {
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            String key = entry.getKey();
            if (key == null) continue;
            if (key.equalsIgnoreCase("Content-Length") || key.equalsIgnoreCase("Content-Type") || key.equalsIgnoreCase("Content-Range")) {
                continue;
            }
            resp.addHeader(key, entry.getValue());
        }
    }

    public static void main(String[] args) throws IOException {
        new VideoStreamProxy();
    }
}
