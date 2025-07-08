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
        map.put("url", "http://10.121.235.6/test/video/spring_boot.mp4");
        map.put("threads", 4);
        map.put("chunkSize", 512 * 1024);
        return map;
    }
}
