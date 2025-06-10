package cn.har01d.httpstreamproxy;

import org.springframework.stereotype.Service;
import java.io.IOException;
import java.net.URI;
import java.net.http.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class VideoStreamingProxyService {
    private static final Logger logger = LoggerFactory.getLogger(VideoStreamingProxyService.class);

    private final int chunkSize = 2 * 1024 * 1024; // 2MB chunks
    private final int maxConcurrency = 10;
    private final HttpClient httpClient;
    private final ExecutorService executor;

    // Headers we should never forward from source to client
    private static final Set<String> SKIPPED_RESPONSE_HEADERS = Set.of(
            "connection", "content-encoding", "content-length",
            "transfer-encoding", "keep-alive", "proxy-authenticate",
            "proxy-authorization", "te", "trailer", "upgrade"
    );

    public VideoStreamingProxyService() {
        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
        this.executor = Executors.newFixedThreadPool(maxConcurrency);
    }

    public void streamVideo(String targetUrl, HttpServletRequest request, HttpServletResponse response) throws IOException {
        try {
            // Get headers to forward to source
            Map<String, String> headersToForward = getHeadersToForward(request);

            // First get headers from source to set up our response
            HttpHeaders sourceHeaders = getSourceHeaders(targetUrl, headersToForward);

            // Set up response headers from source
            setResponseHeaders(response, sourceHeaders);

            // Handle range requests if present
            String rangeHeader = request.getHeader("Range");
            long start = 0;
            long end = Long.parseLong(sourceHeaders.firstValue("Content-Length").orElse("0")) - 1;

            if (rangeHeader != null) {
                String[] ranges = rangeHeader.substring("bytes=".length()).split("-");
                start = Long.parseLong(ranges[0]);
                if (ranges.length > 1) {
                    end = Long.parseLong(ranges[1]);
                }
                response.setStatus(HttpServletResponse.SC_PARTIAL_CONTENT);
                response.setHeader("Content-Range", "bytes " + start + "-" + end + "/" + (end + 1));
            }

            // Stream the video with proper error handling
            try (WritableByteChannel outputChannel = Channels.newChannel(response.getOutputStream())) {
                streamContent(targetUrl, start, end - start + 1, outputChannel, headersToForward, response);
            } catch (IOException e) {
                if (isClientDisconnectError(e)) {
                    logger.debug("Client disconnected during streaming: {}", e.getMessage());
                } else {
                    logger.error("Streaming error", e);
                    throw e;
                }
            }
        } catch (Exception e) {
            if (!isClientDisconnectError(e)) {
                logger.error("Streaming failed", e);
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Streaming failed");
            }
        }
    }

    private HttpHeaders getSourceHeaders(String url, Map<String, String> headers) throws IOException, InterruptedException {
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .method("HEAD", HttpRequest.BodyPublishers.noBody());

        headers.forEach(requestBuilder::header);

        HttpResponse<Void> response = httpClient.send(
                requestBuilder.build(), HttpResponse.BodyHandlers.discarding());

        return response.headers();
    }

    private void setResponseHeaders(HttpServletResponse response, HttpHeaders sourceHeaders) {
        // Set status code
        response.setStatus(HttpServletResponse.SC_OK);

        // Forward all headers except those we want to skip
        sourceHeaders.map().forEach((header, values) -> {
            String lowerHeader = header.toLowerCase();
            if (!SKIPPED_RESPONSE_HEADERS.contains(lowerHeader)) {
                response.setHeader(header, values.get(0));
            }
        });

        // Ensure we have content type
        if (!response.containsHeader("Content-Type")) {
            response.setContentType("application/octet-stream");
        }
    }

    private Map<String, String> getHeadersToForward(HttpServletRequest request) {
        Map<String, String> headers = new HashMap<>();

        // List of headers we want to forward
        Enumeration<String> headerNames = request.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String headerName = headerNames.nextElement();

            // Skip headers we handle specially or don't want to forward
            if (!shouldSkipHeader(headerName)) {
                headers.put(headerName, request.getHeader(headerName));
            }
        }

        return headers;
    }

    private boolean shouldSkipHeader(String headerName) {
        // Skip these headers as we handle them separately
        List<String> skipHeaders = Arrays.asList(
                "host", "connection", "content-length",
                "accept-encoding", "te", "trailer"
        );

        return skipHeaders.contains(headerName.toLowerCase());
    }

    private boolean isClientDisconnectError(Throwable e) {
        return e instanceof IOException &&
                ("Broken pipe".equalsIgnoreCase(e.getMessage()) ||
                        "Connection reset by peer".equalsIgnoreCase(e.getMessage()));
    }

    private void streamContent(String url, long start, long length,
                               WritableByteChannel outputChannel,
                               Map<String, String> headers,
                               HttpServletResponse response) {
        try {
            // Calculate chunks
            int numChunks = (int) ((length + chunkSize - 1) / chunkSize);
            BlockingQueue<VideoChunk> chunkQueue = new LinkedBlockingQueue<>();
            AtomicReference<Exception> error = new AtomicReference<>();
            AtomicLong bytesSent = new AtomicLong(0);

            // Fill queue with chunk definitions
            long remaining = length;
            long position = start;
            for (int i = 0; i < numChunks; i++) {
                long chunkSize = Math.min(this.chunkSize, remaining);
                chunkQueue.add(new VideoChunk(i, position, chunkSize));
                position += chunkSize;
                remaining -= chunkSize;
            }

            // Start download workers
            for (int i = 0; i < maxConcurrency; i++) {
                executor.execute(new DownloadWorker(
                        url, chunkQueue, outputChannel, bytesSent, error,
                        httpClient, headers, response
                ));
            }

            // Wait for completion or error
            while (bytesSent.get() < length && error.get() == null) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            if (error.get() != null && !isClientDisconnectError(error.get())) {
                throw new IOException("Streaming failed", error.get());
            }

        } catch (Exception e) {
            if (!isClientDisconnectError(e)) {
                throw new RuntimeException("Error streaming content", e);
            }
        }
    }

    private static class VideoChunk {
        final int id;
        final long start;
        final long size;
        byte[] data;
        boolean downloaded;

        public VideoChunk(int id, long start, long size) {
            this.id = id;
            this.start = start;
            this.size = size;
        }
    }

    private static class DownloadWorker implements Runnable {
        private final String url;
        private final BlockingQueue<VideoChunk> chunkQueue;
        private final WritableByteChannel outputChannel;
        private final AtomicLong bytesSent;
        private final AtomicReference<Exception> error;
        private final HttpClient httpClient;
        private final Map<String, String> headers;
        private final HttpServletResponse response;

        public DownloadWorker(String url, BlockingQueue<VideoChunk> chunkQueue,
                              WritableByteChannel outputChannel, AtomicLong bytesSent,
                              AtomicReference<Exception> error, HttpClient httpClient,
                              Map<String, String> headers, HttpServletResponse response) {
            this.url = url;
            this.chunkQueue = chunkQueue;
            this.outputChannel = outputChannel;
            this.bytesSent = bytesSent;
            this.error = error;
            this.httpClient = httpClient;
            this.headers = headers;
            this.response = response;
        }

        @Override
        public void run() {
            try {
                while (error.get() == null) {
                    VideoChunk chunk = chunkQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (chunk == null) continue;

                    // Download chunk
                    downloadChunk(chunk);

                    // Write to output
                    writeChunk(chunk);

                    // Update progress
                    bytesSent.addAndGet(chunk.size);
                }
            } catch (Exception e) {
                if (!isClientDisconnectError(e)) {
                    error.compareAndSet(null, e);
                }
            }
        }

        private boolean isClientDisconnectError(Throwable e) {
            return e instanceof IOException &&
                    ("Broken pipe".equalsIgnoreCase(e.getMessage()) ||
                            "Connection reset by peer".equalsIgnoreCase(e.getMessage()));
        }

        private void downloadChunk(VideoChunk chunk) throws IOException, InterruptedException {
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Range", "bytes=" + chunk.start + "-" + (chunk.start + chunk.size - 1));

            // Add all forwarded headers
            headers.forEach(requestBuilder::header);

            HttpRequest request = requestBuilder.GET().build();

            HttpResponse<byte[]> response = httpClient.send(
                    request, HttpResponse.BodyHandlers.ofByteArray());

            if (response.statusCode() != 206) {
                throw new IOException("Invalid status code: " + response.statusCode() +
                        " for range request");
            }

            // Forward any new headers from this chunk's response
//            response.headers().map().forEach((header, values) -> {
//                if (!SKIPPED_RESPONSE_HEADERS.contains(header.toLowerCase())) {
//                    values.forEach(value -> this.response.addHeader(header, value));
//                }
//            });

            chunk.data = response.body();
            chunk.downloaded = true;
        }

        private void writeChunk(VideoChunk chunk) throws IOException {
            try {
                synchronized (outputChannel) {
                    ByteBuffer buffer = ByteBuffer.wrap(chunk.data);
                    while (buffer.hasRemaining()) {
                        outputChannel.write(buffer);
                    }
                }
            } catch (IOException e) {
                if (isClientDisconnectError(e)) {
                    throw e; // Let the outer handler deal with it
                }
                throw new IOException("Error writing chunk " + chunk.id, e);
            }
        }
    }
}