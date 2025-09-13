package com.realtimefinmq.consumer;

import com.realtimefinmq.config.KafkaProps;
import com.realtimefinmq.metrics.KafkaMetricsService;
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
    private final KafkaMetricsService metrics;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaProps kafkaProps;

    @KafkaListener(topics = "#{@kafkaProps.topic}", groupId = "#{@kafkaProps.consumer.groupId}")
    public void consume(ConsumerRecord<String, String> record) {
        String key = record.key();
        String value = record.value();

        // 송신 시간 헤더 읽기
        long sentTs = readLongHeader(record, "ts", -1L);
        if (sentTs < 0) { // 헤더 누락 시 현재 시각으로 대체해 음수/비정상 지연 방지
            log.warn("[Consumer] ts 헤더 없음 | p={} off={}", record.partition(), record.offset());
            sentTs = System.currentTimeMillis(); // fallback
        }
        long latencyMs = System.currentTimeMillis() - sentTs; // 현재 시각 - 송신 시각

        // 메시지 수신 로그
        log.debug("[Consumer] 메시지 수신 | topic={} | partition={} | offset={} | key={} | value={}",
                record.topic(), record.partition(), record.offset(), key, value);

        try {
            // 성공 기록
            metrics.recordMessage(latencyMs);
            metrics.decUncommitted();
            log.debug("[Consumer] 처리 완료 | key={} | latency={}ms", key, latencyMs);

        } catch (Exception e) {
            // 실패 기록 + DLQ 전송
            metrics.recordFailure();
            log.error("[Consumer] 처리 실패 | key={} | 이유={}", key, e.getMessage(), e);

            // DLQ 전송
            String dlqTopic = kafkaProps.getTopic() + ".dlq";
            try {
                kafkaTemplate.send(dlqTopic, key, value); // DLQ로 전송
                metrics.recordDlq();
                log.warn("[Consumer] DLQ 전송 | key={}", key);
            } catch (Exception ex) {
                log.error("[Consumer] DLQ 전송 실패 | key={} | 이유={}", key, ex.getMessage(), ex);
            } finally {
                // 실패/DLQ로 끝났어도 소비 완료이므로 미커밋 -1
                metrics.decUncommitted();
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

