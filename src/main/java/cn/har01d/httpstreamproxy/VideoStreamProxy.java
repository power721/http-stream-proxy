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
    private static final long minChunkSize = 64 * 1024;
    private static final long maxChunkSize = 4 * 1024 * 1024;
    private static final int windowSize = 5;

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
        String id;
        String url;
        int concurrency;
        int chunkSize;
        Map<String, String> headers;
    }

    public static class Session {
        final Video video;

        volatile long dynamicChunkSize;
        final long[] speedWindow;
        int speedIndex = 0;

        long rangeEnd;
        final AtomicLong nextOffset = new AtomicLong();
        final BlockingQueue<Chunk> queue;
        final ExecutorService executor;
        volatile boolean running = true;

        public Session(Video video) {
            this.video = video;
            this.dynamicChunkSize = video.chunkSize;
            this.queue = new LinkedBlockingQueue<>(video.concurrency);
            this.executor = Executors.newFixedThreadPool(video.concurrency);
            this.speedWindow = new long[windowSize];
        }

        public void stop() {
            running = false;
        }

        @Override
        public String toString() {
            return "Session{" +
                    "id='" + video.id + '\'' +
                    ", threads=" + video.concurrency +
                    ", chunkSize=" + dynamicChunkSize +
                    ", nextOffset=" + nextOffset +
                    '}';
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
            return newFixedLengthResponse(Response.Status.OK, "application/json", gson.toJson(sessions));
        }
        if (!uri.startsWith("/proxy/")) {
            return newFixedLengthResponse(Response.Status.NOT_FOUND, MIME_PLAINTEXT, "Not found");
        }

        String id = uri.substring("/proxy/".length());
        for (Map.Entry<String, Session> entry : sessions.entrySet()) {
            entry.getValue().stop();
            sessions.remove(entry.getKey());
        }
        Session session = getSession(id);
        if (session == null) {
            return newFixedLengthResponse(Response.Status.INTERNAL_ERROR, MIME_PLAINTEXT, "Failed to init session");
        }

        try {
            Map<String, String> headers = getOriginalHeaders(session.video.url);
            long totalLength = Long.parseLong(headers.get("Content-Length"));
            String contentType = headers.getOrDefault("Content-Type", "video/mp4");

            String rangeHeader = httpSession.getHeaders().getOrDefault("range", "bytes=0-");
            long rangeStart = parseRangeStart(rangeHeader);
            long rangeEnd = parseRangeEnd(rangeHeader, totalLength - 1);
            long contentLength = rangeEnd - rangeStart + 1;

            long quickStartSize = computeQuickStartSize(totalLength);
            long firstChunkEnd = Math.min(rangeStart + quickStartSize - 1, rangeEnd);
            Chunk firstChunk = new Chunk(rangeStart, firstChunkEnd);
            downloadChunk(session, firstChunk);

            PipedInputStream inPipe = new PipedInputStream((int) Math.max(maxChunkSize, session.video.chunkSize));
            PipedOutputStream outPipe = new PipedOutputStream(inPipe);

            Response response = newFixedLengthResponse(Response.Status.PARTIAL_CONTENT, contentType, inPipe, contentLength);
            response.addHeader("Content-Range", "bytes " + rangeStart + "-" + rangeEnd + "/" + totalLength);
            response.addHeader("Accept-Ranges", "bytes");
            response.addHeader("Access-Control-Allow-Origin", "*");
            copyResponseHeaders(response, headers);

            session.nextOffset.set(firstChunkEnd + 1);
            session.rangeEnd = rangeEnd;
            for (int i = 0; i < session.video.concurrency; i++) {
                startWorker(session);
            }

            new Thread(() -> {
                try (outPipe) {
                    Map<Long, Chunk> buffer = new TreeMap<>();
                    outPipe.write(firstChunk.data);
                    outPipe.flush();
                    long expectedOffset = firstChunk.end + 1;

                    while (session.running) {
                        Chunk chunk = session.queue.poll(100, TimeUnit.MILLISECONDS);
                        if (chunk != null) {
                            buffer.put(chunk.start, chunk);
                        }

                        while (buffer.containsKey(expectedOffset)) {
                            Chunk c = buffer.remove(expectedOffset);
                            if (c.data == null) {
                                session.running = false;
                                break;
                            }
                            outPipe.write(c.data);
                            outPipe.flush();
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
            }, "writer-" + id).start();

            return response;
        } catch (Exception e) {
            log.error("serve failed", e);
            return newFixedLengthResponse(Response.Status.INTERNAL_ERROR, MIME_PLAINTEXT, "Internal error");
        }
    }

    private static long computeQuickStartSize(long totalSize) {
        final long MB = 1024 * 1024;
        final long GB = 1024 * MB;
        if (totalSize >= 2 * GB) {
            return 256 * 1024;
        } else if (totalSize >= 512 * MB) {
            return 128 * 1024;
        } else if (totalSize >= 128 * MB) {
            return 64 * 1024;
        } else {
            return 32 * 1024;
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
                return new Session(video);
            } catch (Exception e) {
                log.warn("Get video info failed!", e);
                return null;
            }
        });
    }

    private void startWorker(Session session) {
        session.executor.submit(() -> {
            while (session.running) {
                long start = session.nextOffset.getAndAdd(session.dynamicChunkSize);
                if (start > session.rangeEnd) {
                    break;
                }
                long end = Math.min(start + session.dynamicChunkSize - 1, session.rangeEnd);
                Chunk chunk = new Chunk(start, end);

                try {
                    long t1 = System.nanoTime();
                    downloadChunk(session, chunk);
                    long t2 = System.nanoTime();
                    long durationMs = TimeUnit.NANOSECONDS.toMillis(t2 - t1);
                    long sizeKB = chunk.data.length / 1024;
                    long speed = durationMs > 0 ? sizeKB * 1000 / durationMs : sizeKB * 1000;
                    adjustChunkSize(session, speed);
                    session.queue.put(chunk);
                } catch (Exception e) {
                    log.warn("Worker failed chunk [{}-{}]: {}", start, end, e.toString());
                    break;
                }
            }
        });
    }

    private void adjustChunkSize(Session session, long speedKBps) {
        synchronized (session) {
            session.speedWindow[session.speedIndex % windowSize] = speedKBps;
            session.speedIndex++;

            if (session.speedIndex < windowSize) {
                return;
            }

            long sum = 0;
            for (int i = 0; i < windowSize; i++) {
                sum += session.speedWindow[i];
            }
            long avgSpeed = sum / windowSize;

            long newChunkSize = session.dynamicChunkSize;
            if (avgSpeed < 200) {
                newChunkSize = Math.max(minChunkSize, session.dynamicChunkSize / 2);
            } else if (avgSpeed > 1500) {
                newChunkSize = Math.min(maxChunkSize, session.dynamicChunkSize * 2);
            }

            if (newChunkSize != session.dynamicChunkSize) {
                log.info("Adjusting chunkSize from {} → {} (avg speed={} KB/s)",
                        session.dynamicChunkSize, newChunkSize, avgSpeed);
                session.dynamicChunkSize = newChunkSize;
            }
        }
    }

    private static void downloadChunk(Session session, Chunk chunk) throws IOException {
        int retries = 3;
        while (retries-- > 0) {
            try {
                HttpURLConnection conn = (HttpURLConnection) new URL(session.video.url).openConnection();
                conn.setRequestProperty("Range", "bytes=" + chunk.start + "-" + chunk.end);
                if (session.video.headers != null) {
                    for (Map.Entry<String, String> entry : session.video.headers.entrySet()) {
                        conn.setRequestProperty(entry.getKey(), entry.getValue());
                    }
                }
                conn.connect();

                int size = (int) (chunk.end - chunk.start + 1);
                ByteBuffer buffer = ByteBuffer.allocate(size);
                try (ReadableByteChannel channel = Channels.newChannel(conn.getInputStream())) {
                    while (buffer.hasRemaining() && channel.read(buffer) > 0) ;
                }
                buffer.flip();
                chunk.data = Arrays.copyOf(buffer.array(), buffer.limit());
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
            if (key.equalsIgnoreCase("Content-Type") || key.equalsIgnoreCase("Content-Range")) {
                continue;
            }
            resp.addHeader(key, entry.getValue());
        }
    }

    public static void main(String[] args) throws IOException {
        new VideoStreamProxy();
    }
}
