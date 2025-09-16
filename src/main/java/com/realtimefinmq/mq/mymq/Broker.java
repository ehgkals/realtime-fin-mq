package com.realtimefinmq.mq.mymq;

import com.realtimefinmq.metrics.MyMqMetricsService;
import com.realtimefinmq.mq.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Broker (MyMQ의 핵심 엔진)
 * - Producer가 보낸 메시지를 큐(InMemoryQueue)에 적재
 * - WAL(Write-Ahead Log)에 기록하여 장애 복구 가능
 * - 멱등성(Idempotency) 체크: 중복 메시지 차단
 * - 큐가 가득 차면 DLQ(Dead Letter Queue)로 메시지 이동
 * - Consumer는 Broker에서 메시지를 꺼내 소비
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class Broker {
    private final InMemoryQueue queue;    // 메인 메시지 큐
    private final IdempotencyStore idem;  // 멱등 저장소 (중복 방지)
    private final MyMqMetricsService metrics; // 지표 집계

    public boolean enqueue(Message msg) {
        try {
            // 멱등성: 이미 본 ID면 거부
            if (idem.alreadyProcessed(msg.getId())) {
                log.warn("[Broker] 중복 메시지 감지 | id={}", msg.getId());
                metrics.recordDuplicate();
                return false;
            }

            // 큐 적재 시도
            boolean ok = queue.offer(msg);
            if (!ok) {
                log.error("[Broker] 큐 꽉참 → DLQ 이동 | id={}", msg.getId());
                return false;
            }

            return true;

        } catch (Exception e) {
            log.error("[Broker] enqueue 실패 | id={} | 이유={}", msg.getId(), e.getMessage(), e);
            metrics.recordFailure();
            return false;
        }
    }

    /**
     * 컨슈머용 poll
     */
    public Message poll(long timeoutMs) {
        return queue.poll(timeoutMs);
    }
}