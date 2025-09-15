package com.realtimefinmq.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    private final ObjectMapper objectMapper;

    /**
     * 금융 거래 메시지를 Kafka로 전송
     * @param payload 보낼 데이터
     */
    public void sendMessage(String payload) {
        if (payload == null || payload.isBlank()) {
            log.warn("[Kafka-Producer] 빈 payload 스킵");
            return;
        }

        final String id = UUID.randomUUID().toString();
        final long ts = System.currentTimeMillis();

        // 메시지 DTO 생성
        Message message = new Message(id, payload, ts, null, null);
        String topic = kafkaProps.getTopic();
        log.debug("[Producer] 메시지 생성 | id: {} | payload: {}", message.getId(), payload);

        // 값은 JSON으로 직렬화 (toString() 사용 지양)
        String value;
        try {
            value = objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            // 직렬화 실패 시 폴백: 최소한 payload라도 보냄
            log.warn("[Kafka-Producer] JSON 직렬화 실패: {} → payload만 전송", e.getMessage());
            value = payload;
        }

        // key를 msgId로 주면 동일 id 기준으로 파티션/순서 고정 가능
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, id, value);

        // 헤더: 생성 시각(ts)과 msgId를 담아 E2E 지연 추적
        record.headers().add("ts", Long.toString(ts).getBytes(StandardCharsets.UTF_8));
        record.headers().add("msgId", id.getBytes(StandardCharsets.UTF_8));

        // 프로듀스 시도 → 소비 전까지 미커밋 +1
        metrics.incUncommitted();

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                // 브로커 수용 실패 → 미커밋 보정 및 실패 집계
                log.error("[Kafka-Producer] 전송 실패 | id:{} | 이유:{}", id, ex.getMessage(), ex);
                metrics.decUncommitted();
                metrics.recordFailure();
            } else {
                log.debug("[Kafka-Producer] 전송 성공 | id:{} | partition:{} | offset:{}",
                        id,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
                // 성공 시에는 컨슈머가 처리 완료할 때 decUncommitted() 호출됨
            }
        });
    }
}
