package com.realtimefinmq.controller;

import com.realtimefinmq.metrics.KafkaMetricsService;
import com.realtimefinmq.metrics.MetricsDto;
import com.realtimefinmq.metrics.MyMqMetricsService;
import com.realtimefinmq.producer.KafkaProducerService;
import com.realtimefinmq.producer.MyMqProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * REST API
 * 실시간 지표 조회용
 */
@RestController
@RequiredArgsConstructor
public class DashboardController {
    private final KafkaMetricsService kafkaMetrics;
    private final MyMqMetricsService myMqMetrics;
    private final KafkaProducerService kafkaProducerService;
    private final MyMqProducerService myMqProducerService;

    @GetMapping("/metrics")
    public Map<String, MetricsDto> kafka() {
        return Map.of(
                "kafka", kafkaMetrics.getMetrics(),
                "mymq", myMqMetrics.getMetrics()
        );
    }

    /** Kafka로 n개 발사 후, 최신 지표 스냅샷 반환 */
    @PostMapping("/metrics/kafka/send")
    public Map<String, Object> sendKafka(@RequestParam(defaultValue = "1000") int n) {
        int count = Math.max(0, n);
        for (int i = 0; i < count; i++) {
            kafkaProducerService.sendMessage("kafka-test-" + i);
        }
        return Map.of(
                "sent", count,
                "target", "kafka",
                "metrics", kafkaMetrics.getMetrics()
        );
    }

    /** MyMQ로 n개 발사 후, 최신 지표 스냅샷 반환 */
    @PostMapping("/metrics/mymq/send")
    public Map<String, Object> sendMyMq(@RequestParam(defaultValue = "1000") int n) {
        int count = Math.max(0, n);
        for (int i = 0; i < count; i++) {
            myMqProducerService.publish("mymq-test-" + i);
        }
        return Map.of(
                "sent", count,
                "target", "mymq",
                "metrics", myMqMetrics.getMetrics()
        );
    }
}
