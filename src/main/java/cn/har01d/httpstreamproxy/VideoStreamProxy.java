package cn.har01d.httpstreamproxy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import fi.iki.elonen.NanoHTTPD;

public class VideoStreamProxy extends NanoHTTPD {
    private static final Logger log = LoggerFactory.getLogger(VideoStreamProxy.class);
    private final ConcurrentMap<String, Session> sessions = new ConcurrentHashMap<>();
    private final Gson gson = new Gson();

    public static class Chunk {
        final long start;
        final long end;
        byte[] data;

        public Chunk(long start, long end) {
            this.start = start;
            this.end = end;
        }
    }

    public static class Video {
        String url;
        int concurrency;
        int chunkSize;
    }

    public static class Session {
        final String videoUrl;
        final long chunkSize;
        final int threadCount;

        long rangeEnd;
        final AtomicLong nextOffset = new AtomicLong();
        final BlockingQueue<Chunk> queue;
        final ExecutorService executor;
        volatile boolean running = true;

        public Session(String videoUrl, int threadCount, long chunkSize) {
            this.videoUrl = videoUrl;
            this.threadCount = threadCount;
            this.chunkSize = chunkSize;
            this.queue = new LinkedBlockingQueue<>(threadCount);
            this.executor = Executors.newFixedThreadPool(threadCount);
        }

        public void stop() {
            running = false;
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
        if (uri.startsWith("/status")) {
            return newFixedLengthResponse(Response.Status.OK, MIME_PLAINTEXT, "");
        }
        if (!uri.startsWith("/proxy/")) {
            return newFixedLengthResponse(Response.Status.NOT_FOUND, MIME_PLAINTEXT, "Not found");
        }

        String id = uri.substring("/proxy/".length());
        if (sessions.containsKey(id)) {
            sessions.get(id).stop();
            sessions.remove(id);
        }
        Session session = getSession(id);
        //log.info("session: {}", session);

        if (session == null) {
            return newFixedLengthResponse(Response.Status.INTERNAL_ERROR, NanoHTTPD.MIME_PLAINTEXT, "Failed to init session");
        }

        try {
            Map<String, String> headers = getOriginalHeaders(session.videoUrl);
            long totalLength = Long.parseLong(headers.get("Content-Length"));
            String contentType = headers.getOrDefault("Content-Type", "video/mp4");

            String rangeHeader = httpSession.getHeaders().getOrDefault("range", "bytes=0-");
            long rangeStart = parseRangeStart(rangeHeader);
            long rangeEnd = parseRangeEnd(rangeHeader, totalLength - 1);
            long contentLength = rangeEnd - rangeStart + 1;

            //log.info("GET {} Range: {} -> {} totalLength={}", uri, rangeStart, rangeEnd, totalLength);

            long quickStartSize = 64 * 1024;
            long firstChunkEnd = Math.min(rangeStart + quickStartSize - 1, rangeEnd);
            Chunk firstChunk = new Chunk(rangeStart, firstChunkEnd);
            downloadChunk(session, firstChunk);
            //log.info("Download chunk [{}-{}]", firstChunk.start, firstChunk.end);

            PipedInputStream inPipe = new PipedInputStream((int) Math.max(4 * 1024 * 1024, session.chunkSize));
            PipedOutputStream outPipe = new PipedOutputStream(inPipe);

            Response response = newFixedLengthResponse(Response.Status.PARTIAL_CONTENT, contentType, inPipe, contentLength);
            response.addHeader("Content-Range", "bytes " + rangeStart + "-" + rangeEnd + "/" + totalLength);
            response.addHeader("Accept-Ranges", "bytes");
            response.addHeader("Access-Control-Allow-Origin", "*");
            copyResponseHeaders(response, headers);

            session.nextOffset.set(firstChunkEnd + 1);
            session.rangeEnd = rangeEnd;
            for (int i = 0; i < session.threadCount; i++) {
                startWorker(session);
            }

            new Thread(() -> {
                try (outPipe) {
                    Map<Long, Chunk> buffer = new TreeMap<>();

                    outPipe.write(firstChunk.data);
                    outPipe.flush();
                    //log.info("first chunk sent");
                    long expectedOffset = firstChunk.end + 1;

                    while (session.running) {
                        Chunk chunk = session.queue.poll(100, TimeUnit.MILLISECONDS);
                        if (chunk != null) {
                            buffer.put(chunk.start, chunk);
                        }

                        while (buffer.containsKey(expectedOffset)) {
                            Chunk c = buffer.remove(expectedOffset);
                            if (c.data == null) {
                                //log.warn("chunk [{}-{}] failed, terminating", c.start, c.end);
                                session.running = false;
                                break;
                            }
                            outPipe.write(c.data);
                            outPipe.flush();
                            //log.info("chunk [{}-{}] sent", c.start, c.end);
                            expectedOffset = c.end + 1;

                            if (c.end >= session.rangeEnd) {
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

            return response;
        } catch (Exception e) {
            log.error("serve failed", e);
            return newFixedLengthResponse(Response.Status.INTERNAL_ERROR, MIME_PLAINTEXT, "Internal error");
        }
    }

    private Session getSession(String id) {
        return sessions.computeIfAbsent(id, key -> {
            try {
                URL restApi = new URL("http://localhost:3000/api/video-info?id=" + id);
                HttpURLConnection conn = (HttpURLConnection) restApi.openConnection();
                conn.setRequestMethod("GET");

                if (conn.getResponseCode() != 200) return null;

                InputStream in = conn.getInputStream();
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                byte[] buffer = new byte[1024];
                int len;
                while ((len = in.read(buffer)) != -1) {
                    out.write(buffer, 0, len);
                }
                String json = out.toString();
                Video video = gson.fromJson(json, Video.class);
                return new Session(video.url, video.concurrency, video.chunkSize);
            } catch (Exception e) {
                log.warn("Get video info failed!", e);
                return null;
            }
        });
    }

    private void startWorker(Session session) {
        session.executor.submit(() -> {
            //log.info("start worker thread");
            while (session.running) {
                long start = session.nextOffset.getAndAdd(session.chunkSize);
                if (start > session.rangeEnd) {
                    break;
                }
                long end = Math.min(start + session.chunkSize - 1, session.rangeEnd);
                Chunk chunk = new Chunk(start, end);

                try {
                    downloadChunk(session, chunk);
                    session.queue.put(chunk);
                } catch (Exception e) {
                    log.warn("Worker failed chunk [{}-{}]: {}", start, end, e.toString());
                    break;
                }
            }
        });
    }

    private static void downloadChunk(Session session, Chunk chunk) throws IOException {
        int retries = 3;
        while (retries-- > 0) {
            try {
                HttpURLConnection conn = (HttpURLConnection) new URL(session.videoUrl).openConnection();
                conn.setRequestProperty("Range", "bytes=" + chunk.start + "-" + chunk.end);
                conn.connect();

                int size = (int) (chunk.end - chunk.start + 1);
                ByteBuffer buffer = ByteBuffer.allocate(size);
                try (ReadableByteChannel channel = Channels.newChannel(conn.getInputStream())) {
                    while (buffer.hasRemaining() && channel.read(buffer) > 0) ;
                }
                buffer.flip();
                chunk.data = Arrays.copyOf(buffer.array(), buffer.limit());
                //log.info("downloaded chunk [{}-{}]", chunk.start, chunk.end);
                return;
            } catch (IOException e) {
                log.warn("Retry download chunk [{}-{}] due to {}", chunk.start, chunk.end, e.toString());
            }
        }
        session.stop();
        throw new IOException("Download failed after retries for chunk [" + chunk.start + "-" + chunk.end + "]");
    }

    private Map<String, String> getOriginalHeaders(String videoUrl) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(videoUrl).openConnection();
        conn.setRequestMethod("HEAD");
        conn.setConnectTimeout(6000);
        conn.setReadTimeout(6000);
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
            if (key.equalsIgnoreCase("Content-Type")
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
