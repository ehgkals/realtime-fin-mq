package com.realtimefinmq.consumer;

import com.realtimefinmq.config.MyMqConfig;
import com.realtimefinmq.metrics.MyMqMetricsService;
import com.realtimefinmq.mq.Message;
import com.realtimefinmq.mq.mymq.Broker;
import com.realtimefinmq.mq.mymq.IdempotencyStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * MyMQ Consumer
 * - Broker에서 메시지를 poll() 해서 처리
 * - KafkaListener와 유사한 역할
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MyMqConsumerService {
    private final MyMqMetricsService metrics; // 성능/운영 지표 기록
    private final IdempotencyStore idempotencyStore; // 멱등 저장소 (성공 후 제거 전략)
    private final Broker broker; // MyMQ 브로커 (큐 + WAL + DLQ)
    private final MyMqConfig cfg; // 폴링/지연 설정 값

    /**
     * 주기적으로 큐에서 메시지를 꺼내 처리
     * - @Scheduled(fixedDelay=100) → 100ms 간격으로 실행
     * - 큐가 비어 있으면 null 반환
     */
    @Scheduled(fixedDelayString = "${custom-mq.poll-interval-ms:100}")
    public void consume() {
        // 메시지 꺼내오기 (최대 50ms 대기)
        Message msg = broker.poll(50);

        if (msg == null) {
            return; // 큐가 비어있으면 종료
        }

        // 지연 계산
        long now = System.currentTimeMillis();
        long latency = now - msg.getTimestamp();
        if (latency < 0) { // 송신/수신 시계 차이 등으로 음수가 나올 수 있으니 보정
            latency = 0;
        }

        boolean decDone = false; // 미커밋 카운트를 정확히 1번만 줄이기 위한 가드

        try {
            log.debug("[MyMQ-Consumer] 메시지 처리 | id={} | payload={} | latency={}ms",
                    msg.getId(), msg.getPayload(), latency);

            // 지표 반영
            metrics.recordMessage(latency);
            idempotencyStore.removeProcessed(msg.getId());
            metrics.decUncommitted();
            decDone = true;

        } catch (Exception e) {
            // 실패 처리
            log.error("[MyMQ-Consumer] 메시지 처리 실패 | id={} | 이유={}", msg.getId(), e.getMessage(), e);
            metrics.recordFailure();
            metrics.decUncommitted();
            decDone = true;
        } finally {
            // 8) 혹시 위에서 -1을 못했으면 여기서 보정 (이중 감소 방지 가드)
            if (!decDone) {
                metrics.decUncommitted();
            }
        }
    }
}