package cn.har01d.httpstreamproxy;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class FileSizeChecker {
    public static long getFileSize(String url) throws IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .method("HEAD", HttpRequest.BodyPublishers.noBody())
                .build();

        HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());

        // Check Content-Length header
        String contentLength = response.headers().firstValue("Content-Length").orElse("-1");
        return Long.parseLong(contentLength);
    }

    private static long getFileSizeByRange(String url) throws IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Range", "bytes=0-0") // Request just first byte
                .GET()
                .build();

        HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());

        return getSize(response);
    }

    private static long getSize(HttpResponse<InputStream> response) {
        // Check Content-Range header (e.g., "bytes 0-0/123456")
        String contentRange = response.headers().firstValue("Content-Range").orElse(null);
        if (contentRange != null) {
            String[] parts = contentRange.split("/");
            if (parts.length > 1) {
                return Long.parseLong(parts[1]);
            }
        }

        // Fallback to Content-Length
        return Long.parseLong(response.headers().firstValue("Content-Length").orElse("-1"));
    }
}
