package com.realtimefinmq.consumer;

import com.realtimefinmq.metrics.MetricsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaConsumerService {
    private final MetricsService metrics;

    @KafkaListener(topics = "financial-transactions", groupId = "finance-mq-group")
    public void consume(ConsumerRecord<String, String> record) {
        String key = record.key();
        String value = record.value();

        // 메시지 수신 로그
        log.info("[Consumer] 메시지 수신 | topic={} | partition={} | offset={} | key={} | value={}",
                record.topic(), record.partition(), record.offset(), key, value);

        try {
            metrics.recordMessage();

            log.info("[Consumer] 메시지 처리 완료 | key={}", key);

        } catch (Exception e) {
            log.error("[Consumer] 메시지 처리 실패 | key={} | 이유={}", key, e.getMessage(), e);
            // MetricsService에 실패 카운터 추가
        }
    }
}

