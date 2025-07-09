package cn.har01d.httpstreamproxy;

import java.util.HashMap;
import java.util.Map;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/video-info")
public class VideoInfoController {
    @GetMapping
    public Map<String, Object> getVideo(String id) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", id);
        String url = "http://10.121.235.6/test/video/spring_boot.mp4";
        if (id.equals("spring")) {
            url = "http://10.121.235.6/test/video/Spring%20Boot%20Microservices.mp4";
        }
        map.put("url", url);
        map.put("concurrency", 4);
        map.put("chunkSize", 512 * 1024);
        map.put("headers", Map.of("referer", "https://example.com"));
        return map;
    }
}
