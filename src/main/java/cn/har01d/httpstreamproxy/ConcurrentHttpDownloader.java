package cn.har01d.httpstreamproxy;

import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.net.http.HttpClient.Version;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class ConcurrentHttpDownloader {
    // Configuration
    private final int partSize;
    private final int maxConcurrency;
    private final int maxRetries;
    private final HttpClient httpClient;
    private final Semaphore globalLimiter;

    // Builder pattern for configuration
    public static class Builder {
        private int partSize = 10 * 1024 * 1024; // 10MB default
        private int maxConcurrency = 4; // Default threads
        private int maxRetries = 3;
        private HttpClient httpClient;
        private Integer globalConcurrencyLimit;

        public Builder partSize(int size) {
            this.partSize = size;
            return this;
        }

        public Builder maxConcurrency(int threads) {
            this.maxConcurrency = threads;
            return this;
        }

        public Builder maxRetries(int retries) {
            this.maxRetries = retries;
            return this;
        }

        public Builder httpClient(HttpClient client) {
            this.httpClient = client;
            return this;
        }

        public Builder globalConcurrencyLimit(int limit) {
            this.globalConcurrencyLimit = limit;
            return this;
        }

        public ConcurrentHttpDownloader build() {
            if (httpClient == null) {
                httpClient = HttpClient.newBuilder()
                        .version(Version.HTTP_2)
                        .followRedirects(HttpClient.Redirect.NORMAL)
                        .build();
            }
            Semaphore limiter = globalConcurrencyLimit != null
                    ? new Semaphore(globalConcurrencyLimit)
                    : null;
            return new ConcurrentHttpDownloader(partSize, maxConcurrency, maxRetries, httpClient, limiter);
        }
    }

    private ConcurrentHttpDownloader(int partSize, int maxConcurrency, int maxRetries,
                                     HttpClient httpClient, Semaphore globalLimiter) {
        this.partSize = partSize;
        this.maxConcurrency = maxConcurrency;
        this.maxRetries = maxRetries;
        this.httpClient = httpClient;
        this.globalLimiter = globalLimiter;
    }

    // Main download method
    public void downloadFile(String url, long fileSize, Path outputPath) throws IOException {
        // Check global concurrency limit
        if (globalLimiter != null) {
            try {
                if (!globalLimiter.tryAcquire(1, 10, TimeUnit.SECONDS)) {
                    throw new IOException("Global concurrency limit exceeded");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Download interrupted while waiting for permit", e);
            }
        }

        try {
            // Calculate chunks
            int numChunks = (int) ((fileSize + partSize - 1) / partSize);
            int actualConcurrency = Math.min(maxConcurrency, numChunks);

            // Create thread pool and shared structures
            ExecutorService executor = Executors.newFixedThreadPool(actualConcurrency);
            BlockingQueue<DownloadChunk> queue = new LinkedBlockingQueue<>();
            AtomicReference<Exception> error = new AtomicReference<>();
            List<DownloadChunk> chunks = new ArrayList<>(numChunks);

            // Initialize chunks
            long remaining = fileSize;
            long position = 0;
            for (int i = 0; i < numChunks; i++) {
                long chunkSize = Math.min(partSize, remaining);
                chunks.add(new DownloadChunk(i, position, chunkSize));
                position += chunkSize;
                remaining -= chunkSize;
            }

            // Fill queue (skip if single chunk)
            if (numChunks > 1) {
                queue.addAll(chunks);
            }

            // Start workers
            for (int i = 0; i < actualConcurrency; i++) {
                executor.execute(new DownloadWorker(
                        queue, httpClient, maxRetries, url, chunks, error
                ));
            }

            // Wait for completion
            executor.shutdown();
            try {
                if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                    executor.shutdownNow();
                    throw new IOException("Download timed out");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Download interrupted", e);
            }

            // Check for errors
            if (error.get() != null) {
                throw new IOException("Download failed", error.get());
            }

            // Write to file
            try (OutputStream out = Files.newOutputStream(outputPath)) {
                for (DownloadChunk chunk : chunks) {
                    out.write(chunk.getData());
                }
            }
        } finally {
            if (globalLimiter != null) {
                globalLimiter.release();
            }
        }
    }

    // Chunk data structure
    private static class DownloadChunk {
        private final int id;
        private final long start;
        private final long size;
        private final ByteArrayOutputStream buffer;
        private final AtomicLong bytesWritten = new AtomicLong();

        public DownloadChunk(int id, long start, long size) {
            this.id = id;
            this.start = start;
            this.size = size;
            this.buffer = new ByteArrayOutputStream((int) size);
        }

        public synchronized void write(byte[] data, int offset, int length) {
            buffer.write(data, offset, length);
            bytesWritten.addAndGet(length);
        }

        public boolean isComplete() {
            return bytesWritten.get() >= size;
        }

        public byte[] getData() {
            return buffer.toByteArray();
        }
    }

    // Worker thread
    private class DownloadWorker implements Runnable {
        private final BlockingQueue<DownloadChunk> queue;
        private final HttpClient httpClient;
        private final int maxRetries;
        private final String url;
        private final List<DownloadChunk> allChunks;
        private final AtomicReference<Exception> error;

        public DownloadWorker(BlockingQueue<DownloadChunk> queue, HttpClient httpClient,
                              int maxRetries, String url, List<DownloadChunk> allChunks,
                              AtomicReference<Exception> error) {
            this.queue = queue;
            this.httpClient = httpClient;
            this.maxRetries = maxRetries;
            this.url = url;
            this.allChunks = allChunks;
            this.error = error;
        }

        @Override
        public void run() {
            try {
                while (error.get() == null) {
                    DownloadChunk chunk = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (chunk == null) {
                        // Check if all chunks are complete
                        if (allChunks.stream().allMatch(DownloadChunk::isComplete)) {
                            break;
                        }
                        continue;
                    }

                    for (int retry = 0; retry <= maxRetries; retry++) {
                        if (downloadChunk(chunk)) {
                            break; // Success
                        }
                        if (retry == maxRetries) {
                            error.compareAndSet(null, new IOException(
                                    "Max retries (" + maxRetries + ") exceeded for chunk " + chunk.id));
                            return;
                        }
                        Thread.sleep(200 * (retry + 1)); // Exponential backoff
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                error.compareAndSet(null, new IOException("Download worker interrupted", e));
            }
        }

        private boolean downloadChunk(DownloadChunk chunk) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .header("Range", "bytes=" + chunk.start + "-" + (chunk.start + chunk.size - 1))
                        .GET()
                        .build();

                HttpResponse<InputStream> response = httpClient.send(
                        request, HttpResponse.BodyHandlers.ofInputStream());

                if (response.statusCode() != 206) {
                    throw new IOException("Invalid status code: " + response.statusCode() +
                            " for range request");
                }

                try (InputStream is = response.body()) {
                    byte[] buffer = new byte[8192];
                    int bytesRead;
                    while ((bytesRead = is.read(buffer)) != -1) {
                        chunk.write(buffer, 0, bytesRead);
                    }
                }

                if (!chunk.isComplete()) {
                    throw new IOException("Incomplete chunk download");
                }

                return true;
            } catch (Exception e) {
                return false;
            }
        }
    }

    // Example usage
    public static void main(String[] args) {
        try {
            ConcurrentHttpDownloader downloader = new ConcurrentHttpDownloader.Builder()
                    .partSize(4 * 1024 * 1024) // 5MB chunks
                    .maxConcurrency(10) // 4 threads
                    .maxRetries(3)
                    .globalConcurrencyLimit(10) // Max 10 concurrent downloads globally
                    .build();

            String url = "http://10.121.235.6/test/numbers.txt";
            long fileSize = FileSizeChecker.getFileSize(url);
            Path outputPath = Paths.get("numbers.txt");

            System.out.println("Starting download...");
            long startTime = System.currentTimeMillis();

            downloader.downloadFile(url, fileSize, outputPath);

            long duration = System.currentTimeMillis() - startTime;
            System.out.printf("Download completed in %.2f seconds%n", duration / 1000.0);

        } catch (Exception e) {
            System.err.println("Download failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
