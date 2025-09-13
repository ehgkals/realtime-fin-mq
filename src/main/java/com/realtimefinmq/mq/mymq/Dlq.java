package com.realtimefinmq.mq.mymq;

import com.realtimefinmq.config.MyMqConfig;
import com.realtimefinmq.metrics.MyMqMetricsService;
import com.realtimefinmq.mq.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * DLQ (Dead Letter Queue)
 * --------------------------
 * - 정상적인 큐에서 처리할 수 없는 메시지를 별도로 보관하는 큐
 * - 예시: 처리 실패, 큐 용량 초과, 예외 발생 등
 *
 * 역할:
 * 1. Broker에서 메시지를 처리하지 못할 때 → DLQ로 이동
 * 2. DLQ에서 모아둔 메시지를 모니터링/재처리 가능
 *
 * 특징:
 * - 최대 크기: 1000
 * - 초과 시 메시지를 버리고 로그로 남김
 */
@Slf4j
@Component
public class Dlq {
    // 내부적으로 BlockingQueue 사용 (FIFO 구조)
    private final BlockingQueue<Message> dlq;
    private final MyMqMetricsService metrics;

    public Dlq(MyMqConfig cfg, MyMqMetricsService metrics) {
        this.dlq = new LinkedBlockingQueue<>(cfg.getDlqSize());
        this.metrics = metrics;
    }

    /**
     * DLQ에 메시지를 추가
     *
     * @param msg 처리 실패한 메시지
     */
    public void add(Message msg) {
        if (dlq.offer(msg)) {
            log.warn("[DLQ] 메시지 추가 | id={}", msg.getId());
            metrics.recordDlq();
        } else {
            log.error("[DLQ] 용량 초과 → 메시지 버려짐 | id={}", msg.getId());
            metrics.recordFailure(); // 용량 초과도 실패로 집계
        }
    }

    /**
     * DLQ에서 메시지를 꺼내기 (소비)
     *
     * @param timeoutMs 대기 시간 (밀리초)
     * @return Message 있으면 반환, 없으면 null
     */
    public Message poll(long timeoutMs) {
        try {
            return dlq.poll(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // 스레드 인터럽트 발생 시 null 반환
            Thread.currentThread().interrupt();
            return null;
        }
    }

    public int size() {
        return dlq.size();
    }

    public boolean isEmpty() {
        return dlq.isEmpty();
    }

    public boolean isFull() {
        return dlq.remainingCapacity() == 0;
    }
}