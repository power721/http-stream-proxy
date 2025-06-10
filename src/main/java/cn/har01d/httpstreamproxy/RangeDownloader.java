package cn.har01d.httpstreamproxy;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.http.HttpServletResponse;
import lombok.Setter;

public class RangeDownloader {
    private static final Logger logger = LoggerFactory.getLogger(RangeDownloader.class);
    // Default configuration constants
    public static final int DEFAULT_PART_SIZE = 10 * 1024 * 1024; // 10MB
    public static final int DEFAULT_CONCURRENCY = 2;
    public static final int DEFAULT_MAX_RETRIES = 3;

    // Configuration
    private final int partSize;
    private final int concurrency;
    private final int maxRetries;
    private final HttpClient httpClient;

    public static class Builder {
        private int partSize = DEFAULT_PART_SIZE;
        private int concurrency = DEFAULT_CONCURRENCY;
        private int maxRetries = DEFAULT_MAX_RETRIES;
        private HttpClient httpClient = HttpClient.newHttpClient();

        public Builder partSize(int size) {
            this.partSize = size;
            return this;
        }

        public Builder concurrency(int concurrency) {
            this.concurrency = concurrency;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder httpClient(HttpClient client) {
            this.httpClient = client;
            return this;
        }

        public RangeDownloader build() {
            return new RangeDownloader(this);
        }
    }

    private RangeDownloader(Builder builder) {
        this.partSize = builder.partSize;
        this.concurrency = builder.concurrency;
        this.maxRetries = builder.maxRetries;
        this.httpClient = builder.httpClient;
    }

    public InputStream download(DownloadParams params) throws IOException {
        DownloadContext context = new DownloadContext(params, this);
        return context.execute();
    }

    private static class DownloadContext {
        private final RangeDownloader downloader;
        private final DownloadParams params;
        private final ExecutorService executor;
        private final BlockingQueue<Chunk> chunkQueue;
        private final List<DownloadBuffer> buffers;
        private final AtomicLong written = new AtomicLong(0);
        private final AtomicReference<Throwable> error = new AtomicReference<>();
        private final AtomicInteger nextChunk = new AtomicInteger(0);
        private final Phaser phaser = new Phaser(1); // Main thread registered
        private volatile long totalSize = -1; // -1 means unknown
        private volatile boolean completed = false;

        public DownloadContext(DownloadParams params, RangeDownloader downloader) {
            this.params = params;
            this.downloader = downloader;
            this.executor = Executors.newFixedThreadPool(downloader.concurrency);
            this.chunkQueue = new LinkedBlockingQueue<>(downloader.concurrency);
            this.buffers = new ArrayList<>();
        }

        public InputStream execute() throws IOException {
            try {
                startWorkers();
                scheduleInitialChunks();

                // Return a stream that will read from the buffers in order
                return new BufferInputStream(buffers, () -> {
                    // Cleanup callback
                    executor.shutdownNow();
                    try {
                        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                            executor.shutdownNow();
                        }
                    } catch (InterruptedException e) {
                        executor.shutdownNow();
                        Thread.currentThread().interrupt();
                    }
                    phaser.arriveAndDeregister();
                });
            } catch (Exception e) {
                executor.shutdownNow();
                throw new IOException("Failed to start download", e);
            }
        }

        private void startWorkers() {
            for (int i = 0; i < downloader.concurrency; i++) {
                phaser.register();
                executor.submit(() -> {
                    try {
                        workerLoop();
                    } finally {
                        phaser.arriveAndDeregister();
                    }
                });
            }
        }

        private void scheduleInitialChunks() throws IOException, InterruptedException {
            long pos = params.range.start();
            int chunkId = 0;

            long fileSize = params.fileSize;
            if (fileSize <= 0) {
                fileSize = getFileSizeByRange();
            }

            if (params.range.length() <= 0) {
                totalSize = fileSize;
            } else {
                totalSize = params.range.length();
            }

            long remaining = totalSize - params.range.start();
            while (remaining > 0 && error.get() == null) {
                long chunkSize = Math.min(remaining, downloader.partSize);
                DownloadBuffer buffer = new DownloadBuffer((int) chunkSize);
                buffers.add(buffer);

                try {
                    chunkQueue.put(new Chunk(chunkId++, pos, chunkSize, buffer));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    error.set(e);
                    break;
                }
                pos += chunkSize;
                remaining -= chunkSize;
            }
        }

        private void workerLoop() {
            try {
                while (!completed && error.get() == null) {
                    Chunk chunk = chunkQueue.poll(200, TimeUnit.MILLISECONDS);
                    if (chunk == null) {
                        continue;
                    }

                    downloadChunk(chunk, chunk.buffer);
                    nextChunk.incrementAndGet();
                }
                System.out.println(Thread.currentThread().getName() + " Worker loop done");
            } catch (InterruptedException e) {
                //System.out.println(Thread.currentThread().getName() + " interrupt");
                Thread.currentThread().interrupt();
                error.set(e);
            }
        }

        private void downloadChunk(Chunk chunk, DownloadBuffer buffer) {
            int retries = 0;
            long bytesDownloaded = 0;

            while (retries <= downloader.maxRetries && error.get() == null) {
                try {
                    //System.out.println(Thread.currentThread().getName() + " download chunk " + chunkId);
                    HttpRequest request = createRequest(chunk, bytesDownloaded);
                    HttpResponse<InputStream> response = downloader.httpClient.send(
                            request, HttpResponse.BodyHandlers.ofInputStream());

                    if (response.statusCode() != 200 && response.statusCode() != 206) {
                        throw new IOException("HTTP request failed with status: " + response.statusCode());
                    }

                    long copied = copyToBuffer(response.body(), buffer);
                    bytesDownloaded += copied;
                    written.addAndGet(copied);

                    //System.out.println("Chunk " + chunkId + " copied: " + copied + " bytesDownloaded: " + bytesDownloaded);

                    if (copied < buffer.capacity()) {
                        buffer.markComplete();
                        completed = true;
                        logger.info("Chunk {} EOF reached", chunk.id);
                        break;
                    }

                    if (bytesDownloaded == buffer.capacity()) {
                        buffer.markComplete();
                        logger.info("Chunk {} completed", chunk.id);
                        break; // Chunk completed
                    }

                    // Partial download - adjust range for next attempt ???
                    retries++;
                } catch (Exception e) {
                    retries++;
                    if (retries > downloader.maxRetries) {
                        error.set(e);
                        break;
                    }
                    try {
                        Thread.sleep(1000 * retries); // Exponential backoff
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        error.set(ie);
                        break;
                    }
                }
            }
        }

        private HttpRequest.Builder newHttpBuilder() {
            var builder = HttpRequest.newBuilder().uri(URI.create(params.url));
            for (var header : params.headers.entrySet()) {
                builder.header(header.getKey(), header.getValue());
            }
            return builder;
        }

        private HttpRequest createRequest(Chunk chunk, long bytesDownloaded) {
            long start = chunk.start + bytesDownloaded;
            long length = chunk.size - bytesDownloaded;

            String rangeHeader = "bytes=" + start + "-" + (start + length - 1);

            return newHttpBuilder()
                    .uri(URI.create(params.url))
                    .header("Range", rangeHeader)
                    .GET()
                    .build();
        }

        private long getFileSize() throws IOException, InterruptedException {
            HttpRequest request = newHttpBuilder()
                    .method("HEAD", HttpRequest.BodyPublishers.noBody())
                    .build();

            HttpResponse<Void> response = downloader.httpClient.send(request, HttpResponse.BodyHandlers.discarding());

            System.out.println("Status code: " + response.statusCode());

            if (response.statusCode() == 405) {
                return getFileSizeByRange();
            }

            response.headers().map().forEach((key, value) -> {
                System.out.println(key + ": " + value);
            });

            // Check Content-Length header
            String contentLength = response.headers().firstValue("Content-Length").orElse("-1");
            return Long.parseLong(contentLength);
        }

        private long getFileSizeByRange() throws IOException, InterruptedException {
            HttpRequest request = newHttpBuilder()
                    .header("Range", "bytes=0-0") // Request just first byte
                    .GET()
                    .build();

            HttpResponse<InputStream> response = downloader.httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());

            System.out.println("Status code: " + response.statusCode());

            if (params.response != null) {
                params.response.setStatus(response.statusCode());
            }

            response.headers().map().forEach((key, values) -> {
                System.out.println(key + ": " + values);
            });

            // Check Content-Range header (e.g., "bytes 0-0/123456")
            String contentRange = response.headers().firstValue("Content-Range").orElse(null);
            if (contentRange != null) {
                String[] parts = contentRange.split("/");
                if (parts.length > 1) {
                    long length = Long.parseLong(parts[1]);
                    response.headers().map().forEach((key, values) -> {
                        if (params.response != null) {
                            String value = values.get(0);
                            if ("content-length".equals(key)) {
                                value = parts[1];
                            } else if ("content-range".equals(key)) {
                                value = "bytes " + params.range.start + "-" + length + "/" + length;
                            }
                            if (!"connection".equals(key)) {
                                params.response.setHeader(key, value);
                            }
                        }
                    });

                    return length;
                }
            }

            // Fallback to Content-Length
            return Long.parseLong(response.headers().firstValue("Content-Length").orElse("-1"));
        }

        private long copyToBuffer(InputStream source, DownloadBuffer buffer) throws IOException {
            byte[] temp = new byte[8192];
            long total = 0;
            int read;

            while ((read = source.read(temp)) > 0) {
                buffer.write(temp, 0, read);
                total += read;
            }

            //System.out.println("buffer " + buffer.id + " write total " + total);
            source.close();
            return total;
        }
    }

    @Setter
    public static class DownloadParams {
        public final String url;
        public final Range range;
        public final long fileSize; // -1 means unknown
        private final Map<String, String> headers = new HashMap<>();
        private HttpServletResponse response;

        public DownloadParams(String url) {
            this.url = url;
            this.range = new Range(0, -1);
            this.fileSize = -1;
        }

        public DownloadParams(String url, Range range) {
            this.url = url;
            this.range = range;
            this.fileSize = -1;
        }

        public DownloadParams(String url, Range range, long fileSize) {
            this.url = Objects.requireNonNull(url, "URL cannot be null");
            this.range = Objects.requireNonNull(range, "Range cannot be null");
            this.fileSize = fileSize;
        }
    }

    public record Range(long start, long length) {
        public Range {
            if (start < 0) throw new IllegalArgumentException("Start cannot be negative");
        }

        public static Range parse(String range) {
            if (range == null || range.isBlank()) {
                return new Range(0, -1);
            }
            String[] parts = range.split("=");
            if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid range: " + range);
            }
            range = parts[1];
            parts = range.split("-");
            int start = 0;
            if (parts.length > 0) {
                start = Integer.parseInt(parts[0]);
            }
            int length = -1;
            if (parts.length == 2) {
                length = Integer.parseInt(parts[1]);
            }
            return new Range(start, length);
        }
    }

    private record Chunk(int id, long start, long size, DownloadBuffer buffer) {
    }

    private static class DownloadBuffer {
        private final ByteBuffer buffer;
        private final int capacity;
        private final Object lock = new Object();
        private volatile boolean closed = false;
        private volatile boolean complete = false;

        public DownloadBuffer(int capacity) {
            this.capacity = capacity;
            this.buffer = ByteBuffer.allocate(capacity);
        }

        public void write(byte[] data, int offset, int length) {
            synchronized (lock) {
                buffer.put(data, offset, length);
                lock.notifyAll();
            }
        }

        public void markComplete() {
            synchronized (lock) {
                complete = true;
                lock.notifyAll();
            }
        }

        public int read(byte[] dest) throws InterruptedException {
            synchronized (lock) {
                while (buffer.position() == 0 && !complete && !closed) {
                    lock.wait(200);
                }

                if (closed) {
                    return -1;
                }

                if (buffer.position() > 0) {
                    buffer.flip();
                    int remaining = buffer.remaining();
                    int toRead = Math.min(remaining, dest.length);
                    buffer.get(dest, 0, toRead);
                    buffer.compact();
                    return toRead;
                }

                return complete && buffer.position() == 0 ? -1 : 0;
            }
        }

        public void close() {
            synchronized (lock) {
                closed = true;
                lock.notifyAll();
            }
        }

        public int capacity() {
            return capacity;
        }
    }

    private static class BufferInputStream extends InputStream {
        private final List<DownloadBuffer> buffers;
        private final Runnable onClose;
        private int currentBufferIndex = 0;
        private int totalRead = 0;

        public BufferInputStream(List<DownloadBuffer> buffers, Runnable onClose) {
            this.buffers = buffers;
            this.onClose = onClose;
        }

        @Override
        public int read() throws IOException {
            byte[] b = new byte[1];
            int read = read(b);
            return read == -1 ? -1 : b[0] & 0xFF;
        }

        @Override
        public int read(byte[] b) throws IOException {
            return read(b, 0, b.length);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            try {
                while (currentBufferIndex < buffers.size()) {
                    DownloadBuffer buffer = buffers.get(currentBufferIndex);
                    int read = buffer.read(b);
                    if (read > 0) {
                        totalRead += read;
                        return read;
                    } else if (read == -1) {
                        logger.info("reached end of stream: {}", currentBufferIndex);
                        currentBufferIndex++;
                        //System.out.println("read next buffer: " + currentBufferIndex + " / " + buffers.size());
                    }
                }
                //logger.info("Total: {} bytes", totalRead);
                System.out.println("Total: " + totalRead + " bytes");
                return -1;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted during read", e);
            }
        }

        @Override
        public void close() {
            if (onClose != null) {
                onClose.run();
            }
            for (DownloadBuffer buffer : buffers) {
                buffer.close();
            }
        }
    }
}
