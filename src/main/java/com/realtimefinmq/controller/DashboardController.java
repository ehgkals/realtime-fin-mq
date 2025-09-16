package com.realtimefinmq.controller;

import com.realtimefinmq.consumer.MyMqConsumerService;
import com.realtimefinmq.metrics.KafkaMetricsService;
import com.realtimefinmq.metrics.MetricsDto;
import com.realtimefinmq.metrics.MyMqMetricsService;
import com.realtimefinmq.producer.KafkaProducerService;
import com.realtimefinmq.producer.MyMqProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST API
 * 실시간 지표 조회용
 */
@CrossOrigin(origins = "*")
@RestController
@RequiredArgsConstructor
public class DashboardController {
    private final KafkaMetricsService kafkaMetrics;
    private final MyMqMetricsService myMqMetrics;
    private final KafkaProducerService kafkaProducerService;
    private final MyMqProducerService myMqProducerService;
    private final MyMqConsumerService myMqConsumerService;

    @GetMapping("/metrics")
    public Map<String, MetricsDto> kafka() {
        return Map.of(
                "kafka", kafkaMetrics.getMetrics(),
                "mymq", myMqMetrics.getMetrics()
        );
    }

    @GetMapping("/metrics/window")
    public Map<String, MetricsDto> metricsWindow(@RequestParam(name = "windowMs", defaultValue = "60000") long windowMs) {
        return Map.of(
                "kafka", kafkaMetrics.getWindowMetrics(windowMs),
                "mymq",  myMqMetrics.getWindowMetrics(windowMs)
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
    public Map<String, Object> sendMyMq(
            @RequestParam(defaultValue = "1000") int n,
            @RequestParam(required = false) String key,
            @RequestParam(defaultValue = "16") int keyBuckets
    ) {
        int count = Math.max(0, n);
        int buckets = Math.max(1, keyBuckets);

        for (int i = 0; i < count; i++) {
            String effectiveKey = (key != null && !key.isBlank())
                    ? key
                    : "key-" + (i % buckets);
            myMqProducerService.publish(effectiveKey, "mymq-test-" + i);
        }
        return Map.of(
                "sent", count,
                "target", "mymq",
                "metrics", myMqMetrics.getMetrics()
        );
    }

    @PostMapping("/metrics/both/send")
    public Map<String, Object> sendBoth(@RequestParam(defaultValue = "1000") int n) {
        int count = Math.max(0, n);
        for (int i = 0; i < count; i++) {
            kafkaProducerService.sendMessage("kafka-test-" + i);
            myMqProducerService.publish("key-" + (i % 16), "mymq-test-" + i);
        }
        return Map.of(
                "sentKafka", count,
                "sentMyMq", count,
                "target", "both",
                "metrics", Map.of(
                        "kafka", kafkaMetrics.getMetrics(),
                        "mymq",  myMqMetrics.getMetrics()
                )
        );
    }

    /** 지표 리셋 (scope=all | latency) */
    @PostMapping("/metrics/reset")
    public Map<String, Object> reset(@RequestParam(defaultValue = "all") String scope) {
        switch (scope.toLowerCase()) {
            case "latency" -> {
                kafkaMetrics.resetLatencyWindow();
                myMqMetrics.resetLatencyWindow();
            }
            case "all" -> {
                kafkaMetrics.resetAll();
                myMqMetrics.resetAll();
                myMqConsumerService.resetConsistencyWindows();
            }
            default -> throw new IllegalArgumentException("scope must be one of: all, latency");
        }
        return Map.of(
                "status", "ok",
                "scope", scope,
                "metrics", Map.of(
                        "kafka", kafkaMetrics.getMetrics(),
                        "mymq",  myMqMetrics.getMetrics()
                )
        );
    }
}
