package cn.har01d.httpstreamproxy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@Controller
public class ProxyController {
    private final VideoStreamingProxyService proxyService;

    public ProxyController(VideoStreamingProxyService proxyService) {
        this.proxyService = proxyService;
    }

    @GetMapping("/proxy")
    public void proxy(String url, HttpServletRequest request, HttpServletResponse response) throws IOException {
        proxyService.streamVideo(url, request, response);

//        String rangeHeader = request.getHeader("Range");
//
//        // Pre-fetch to get headers
//        HttpClient client = HttpClient.newHttpClient();
//        HttpRequest headRequest = HttpRequest.newBuilder()
//                .uri(URI.create(url))
//                .header("Range", "bytes=0-0")
//                .method("GET", HttpRequest.BodyPublishers.noBody()) // Or HEAD if server supports it
//                .build();
//        HttpResponse<Void> headResponse;
//        try {
//            headResponse = client.send(headRequest, HttpResponse.BodyHandlers.discarding());
//        } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//            throw new IOException(e);
//        }
//
//        headResponse.headers().map().forEach((key, values) -> {
//            if (!"connection".equals(key)) {
//                response.setHeader(key, values.get(0));
//            }
//        });
//
//        long fileSize = getSize(headResponse);
//
//        RangeDownloader.DownloadParams params = new RangeDownloader.DownloadParams(url, RangeDownloader.Range.parse(rangeHeader), fileSize);
//        //params.setResponse(response);
//
//        long end = params.range.length() == -1 ? fileSize : params.range.length();
//        boolean partialContent = rangeHeader != null;
//        if (partialContent) {
//            response.setStatus(HttpServletResponse.SC_PARTIAL_CONTENT);
//            response.setHeader("Content-Range", "bytes " + params.range.start() + "-" + end + "/" + fileSize);
//        } else {
//            response.setStatus(HttpServletResponse.SC_OK);
//        }
//        response.setHeader("Content-Length", String.valueOf(end - params.range.start()));
//        response.setHeader("Accept-Ranges", "bytes");
//
//        RangeDownloader downloader = new RangeDownloader.Builder()
//                .concurrency(10)
//                .partSize(400 * 1024)
//                .build();
//
//        OutputStream out = response.getOutputStream();
//        try (InputStream in = downloader.download(params)) {
//            byte[] buffer = new byte[32 * 1024];
//            int read;
//            while ((read = in.read(buffer)) != -1) {
//                out.write(buffer, 0, read);
//            }
//        }
    }


    private static long getSize(HttpResponse<?> response) {
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
