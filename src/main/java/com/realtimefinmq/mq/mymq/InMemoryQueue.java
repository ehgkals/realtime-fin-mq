package com.realtimefinmq.mq.mymq;

import com.realtimefinmq.config.MyMqConfig;
import com.realtimefinmq.mq.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * InMemoryQueue
 * - MyMQ에서 사용하는 실제 "메모리 기반 메시지 큐"
 * - Java의 BlockingQueue(LinkedBlockingQueue) 기반
 * - Producer → offer()로 메시지 적재
 * - Consumer → poll()로 메시지 가져오기
 */
@Slf4j
@Component
public class InMemoryQueue {
    // 내부 큐 (최대 10,000개의 메시지를 저장 가능)
    private final BlockingQueue<Message> queue;

    public InMemoryQueue(MyMqConfig cfg) {
        this.queue = new LinkedBlockingQueue<>(cfg.getQueueSize());
    }

    /**
     * 메시지를 큐에 넣기 (Producer 호출)
     *
     * @param msg 메시지
     * @return true → 성공적으로 큐에 삽입됨 / false → 큐가 가득 차서 삽입 실패
     */
    public boolean offer(Message msg) {
        boolean check = queue.offer(msg);
        if (!check) {
            log.debug("[InMemoryQueue] 큐 가득참 -> 삽입 실패 | id={}", msg.getId());
        }
        return check;
    }

    /**
     * 큐에서 메시지 꺼내기 (Consumer 호출)
     *
     * @param timeoutMs 대기 시간 (밀리초)
     * @return 메시지 (없으면 null)
     */
    public Message poll(long timeoutMs) {
        try {
            return queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("[InMemoryQueue] poll 인터럽트 발생", e);
            return null;
        }
    }

    /**
     * 현재 큐에 쌓여있는 메시지 개수
     *
     * @return 큐 크기
     */
    public int size() {
        return queue.size();
    }
}
