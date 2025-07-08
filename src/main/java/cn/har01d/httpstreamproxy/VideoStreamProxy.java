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
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fi.iki.elonen.NanoHTTPD;

public class VideoStreamProxy extends NanoHTTPD {
    private static final Logger log = LoggerFactory.getLogger(VideoStreamProxy.class);
    private static final Object lock = new Object();

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
        final int threads;
        final long chunkSize;
        final BlockingQueue<Chunk> queue;
        final ExecutorService executor;

        boolean running = true;
        long totalLength;
        int id;

        public Session(String videoUrl, int threads, int chunkSize) {
            this.videoUrl = videoUrl;
            this.threads = threads;
            this.chunkSize = chunkSize;
            queue = new LinkedBlockingQueue<>(threads);
            executor = Executors.newFixedThreadPool(threads);
        }
    }

    private final ConcurrentMap<String, Session> sessions = new ConcurrentHashMap<>();

    public VideoStreamProxy() throws IOException {
        super(9000);
        start(NanoHTTPD.SOCKET_READ_TIMEOUT, false);
        System.out.println("Proxy started at http://localhost:9000");
    }

    @Override
    public Response serve(IHTTPSession session) {
        String uri = session.getUri();
        if (!uri.startsWith("/proxy/")) {
            return newFixedLengthResponse(Response.Status.NOT_FOUND, NanoHTTPD.MIME_PLAINTEXT, "Not found");
        }

        String id = uri.substring("/proxy/".length());

        try {
            Session vidSession = sessions.computeIfAbsent(id, key -> {
                return new Session("http://10.121.235.6/test/video/spring_boot.mp4", 4, 512 * 1024);
            });

            String range = session.getHeaders().get("range");
            if (range == null || !range.startsWith("bytes=")) {
                range = "bytes=0-";
            }
            long rangeStart = Long.parseLong(range.replace("bytes=", "").split("-")[0]);

            Map<String, String> headers = getOriginalHeaders(vidSession.videoUrl);
            if (!headers.containsKey("Content-Length")) {
                return newFixedLengthResponse(Response.Status.INTERNAL_ERROR, MIME_PLAINTEXT, "Cannot get video length");
            }

            long totalLength = Long.parseLong(headers.get("Content-Length"));
            vidSession.totalLength = totalLength;
            long contentLength = totalLength - rangeStart + 1;
            String contentType = headers.getOrDefault("Content-Type", "video/mp4");
            log.info("totalLength: {} contentLength: {} contentType: {}", totalLength, contentLength, contentType);

            PipedInputStream inPipe = new PipedInputStream(64 * 1024);
            PipedOutputStream outPipe = new PipedOutputStream(inPipe);

            Response resp = newFixedLengthResponse(Response.Status.PARTIAL_CONTENT, contentType,
                    inPipe, contentLength);

            responseHeaders(resp, headers);
            resp.addHeader("Content-Range", "bytes " + rangeStart + "-" + (totalLength - 1) + "/" + totalLength);

            for (int i = 0; i < vidSession.threads; i++) {
                startWorker(vidSession, rangeStart);
            }

            new Thread(() -> {
                try {
                    while (vidSession.running) {
                        Chunk c = vidSession.queue.poll(100, TimeUnit.MILLISECONDS);
                        if (c == null) continue;

                        while (vidSession.running && c.data == null) {
                            Thread.sleep(10);
                        }

                        try {
                            outPipe.write(c.data);
                        } catch (IOException e) {
                            log.info("Client disconnected, stopping stream");
                            vidSession.running = false;
                            break;
                        }
                    }
                } catch (Exception ex) {
                    log.error("write failed", ex);
                } finally {
                    try {
                        outPipe.close();
                    } catch (IOException ignored) {
                    }
                    sessions.remove(id);
                }
            }).start();

            return resp;
        } catch (Exception e) {
            log.warn("serve failed", e);
            return newFixedLengthResponse(Response.Status.INTERNAL_ERROR, NanoHTTPD.MIME_PLAINTEXT, "Error");
        }
    }

    private static void responseHeaders(Response resp, Map<String, String> headers) {
        for (Map.Entry<String, String> header : headers.entrySet()) {
            if (header.getKey() == null
                    || header.getKey().equals("Content-Range")
                    || header.getKey().equals("Content-Type")) {
                continue;
            }
            log.info("header: {}={}", header.getKey(), header.getValue());
            resp.addHeader(header.getKey(), header.getValue());
        }
    }

    private Map<String, String> getOriginalHeaders(String videoUrl) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(videoUrl).openConnection();
        conn.setRequestMethod("HEAD");
        conn.setConnectTimeout(3000);
        conn.setReadTimeout(3000);
        conn.connect();
        Map<String, String> headers = new LinkedHashMap<>();
        if (conn.getResponseCode() / 100 == 2) {
            conn.getHeaderFields().forEach((k, v) -> {
                headers.put(k, v.get(0));
            });
            headers.put("Content-Length", String.valueOf(conn.getContentLengthLong()));
        }
        return headers;
    }

    private static void startWorker(Session vidSession, long rangeStart) {
        vidSession.executor.submit(() -> {
            while (vidSession.running) {
                int cid = 0;
                try {
                    boolean completed = false;
                    Chunk chunk;
                    synchronized (lock) {
                        cid = vidSession.id++;
                        long start = rangeStart + cid * vidSession.chunkSize;
                        long end = Math.min(start + vidSession.chunkSize - 1, vidSession.totalLength - 1);
                        completed = end == vidSession.totalLength - 1;
                        chunk = new Chunk(cid, start, end);
                    }

                    downloadChunk(vidSession.videoUrl, chunk);
                    vidSession.queue.put(chunk);

                    if (completed) break;

                } catch (IOException e) {
                    log.warn("download chunk {} failed", cid, e);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
    }

    private static void downloadChunk(String urlStr, Chunk chunk) throws IOException {
        log.info("download chunk: {} [{}-{}]", chunk.id, chunk.start, chunk.end);
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

    public static void main(String[] args) throws IOException {
        new VideoStreamProxy();
    }
}
