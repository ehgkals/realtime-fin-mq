package com.realtimefinmq.producer;

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
public class Producer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    // Kafka 토픽 이름
    private static final String TOPIC = "financial-transactions";

    /**
     * 금융 거래 메시지를 Kafka로 전송
     * @param payload 보낼 데이터
     */
    public void sendMessage(String payload) {
        // 메시지 DTO 생성
        Message message = new Message(UUID.randomUUID().toString(), payload, System.currentTimeMillis());
        log.info("[Producer] 메시지 생성 | id: {} | payload: {}", message.getId(), payload);

        ProducerRecord<String, String> record =
                new ProducerRecord<>(TOPIC, null, message.getId(), message.toString());

        record.headers().add("ts",
                Long.toString(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));  // 전송 시각
        record.headers().add("msgId",
                message.getId().getBytes(StandardCharsets.UTF_8));

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[Producer] 전송 실패 | id:{} | 이유:{}", message.getId(), ex.getMessage(), ex);
            } else {
                log.info("[Producer] 전송 성공 | id:{} | partition:{} | offset:{}",
                        message.getId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        // 전송 성공/실패 여부 로그
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[Producer] 메시지 전송 실패 | id: {} | topic: {} | 이유: {}", message.getId(), TOPIC, ex.getMessage(), ex);
            } else {
                log.info("[Producer] 메시지 전송 성공 | id: {} | topic: {} | partition: {} | offset: {}", message.getId(), TOPIC, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            }
        });
    }
}
