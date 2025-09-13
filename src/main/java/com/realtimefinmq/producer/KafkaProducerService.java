package com.realtimefinmq.producer;

import com.realtimefinmq.config.KafkaProps;
import com.realtimefinmq.metrics.KafkaMetricsService;
import com.realtimefinmq.mq.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaMetricsService metrics;
    private final KafkaProps kafkaProps;

    /**
     * 금융 거래 메시지를 Kafka로 전송
     * @param payload 보낼 데이터
     */
    public void sendMessage(String payload) {
        if (payload == null || payload.isBlank()) {
            log.warn("[Kafka-Producer] 빈 payload 스킵");
            return;
        }

        // 메시지 DTO 생성
        Message message = new Message(UUID.randomUUID().toString(), payload, System.currentTimeMillis());
        String topic = kafkaProps.getTopic();
        log.debug("[Producer] 메시지 생성 | id: {} | payload: {}", message.getId(), payload);

        // ProducerRecord 구성 (key=id → 파티셔닝/순서 보장)
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, message.getId(), message.toString());

        // 헤더: 생성 시각(ts)과 msgId를 담아 E2E 지연 추적
        record.headers().add("ts", Long.toString(message.getTimestamp()).getBytes(StandardCharsets.UTF_8));
        record.headers().add("msgId", message.getId().getBytes(StandardCharsets.UTF_8));

        metrics.incUncommitted();

        // 전송
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);

        // 전송 성공/실패 여부 로그
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[Producer] 전송 실패 | id:{} | 이유:{}", message.getId(), ex.getMessage(), ex);
                metrics.decUncommitted();
                metrics.recordFailure();
            } else {
                log.debug("[Producer] 전송 성공 | id:{} | partition:{} | offset:{}",
                        message.getId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}
