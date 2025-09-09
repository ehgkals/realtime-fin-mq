package com.realtimefinmq.consumer;

import com.realtimefinmq.metrics.MetricsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaConsumerService {
    private final MetricsService metrics;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final String DLQ_TOPIC = "financial-transactions.dlq"; // DLQ 전송용 KafkaTemplate

    @KafkaListener(topics = "financial-transactions", groupId = "finance-mq-group")
    public void consume(ConsumerRecord<String, String> record) {
        String key = record.key();
        String value = record.value();

        long sentTs = readLongHeader(record, "ts", System.currentTimeMillis());
        long latencyMs = System.currentTimeMillis() - sentTs; // 현재 시각 - 송신 시각


        // 메시지 수신 로그
        log.info("[Consumer] 메시지 수신 | topic={} | partition={} | offset={} | key={} | value={}",
                record.topic(), record.partition(), record.offset(), key, value);

        try {
            log.info("[Consumer] 수신 | p={} | off={} | key={}", record.partition(), record.offset(), key);

            // 비즈니스 처리 로직

            // 성공 기록
            metrics.recordMessage(latencyMs);
            log.error("[Consumer] 처리 완료 | key={} | latency={}ms", key, latencyMs);

        } catch (Exception e) {
            // 실패 기록 + DLQ 전송
            metrics.recordFailure();
            log.error("[Consumer] 처리 실패 | key={} | 이유={}", key, e.getMessage(), e);
            try {
                kafkaTemplate.send(DLQ_TOPIC, key, value); // DLQ로 전송
                metrics.recordDlq();
                log.warn("[Consumer] DLQ 전송 | key={}", key);
            } catch (Exception ex) {
                log.error("[Consumer] DLQ 전송 실패 | key={} | 이유={}", key, ex.getMessage(), ex);
            }
        }
    }

    private long readLongHeader(ConsumerRecord<String, String> rec, String k, long def) {
        Header h = rec.headers().lastHeader(k);
        if (h == null) return def;
        try { return Long.parseLong(new String(h.value(), StandardCharsets.UTF_8)); }
        catch (Exception ignore) { return def; }
    }
}

