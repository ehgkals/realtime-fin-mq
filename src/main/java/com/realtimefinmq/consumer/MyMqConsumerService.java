package com.realtimefinmq.consumer;

import com.realtimefinmq.config.MyMqConfig;
import com.realtimefinmq.metrics.MyMqMetricsService;
import com.realtimefinmq.mq.Message;
import com.realtimefinmq.mq.mymq.Broker;
import com.realtimefinmq.mq.mymq.IdempotencyStore;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

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

    // ===== 중복 감지용(최근 N개 윈도우) =====
    private static final int DEDUPE_WINDOW_SIZE = 100_000;
    private final ConcurrentMap<String, Boolean> processedIds = new ConcurrentHashMap<>();
    private final ArrayDeque<String> processedOrder = new ArrayDeque<>(DEDUPE_WINDOW_SIZE);
    private final Object dedupeLock = new Object(); // 큐 축출 동기화용

    // ===== 순서 위반 감지용: key별 마지막 seq =====
    private final ConcurrentMap<String, Long> lastSeqByKey = new ConcurrentHashMap<>();

    private Thread worker;
    private volatile boolean running = true;

    /**
     * 메시지 ID가 중복인지 검사하고, 처음 본 ID면 윈도우에 등록한다.
     * true  = 처음 본 것(정상 처리 진행)
     * false = 이미 처리된 것(중복 → 드롭)
     */
    private boolean checkAndRemember(String messageId) {
        if (messageId == null) return true; // ID 없으면 중복 판별 불가 → 통과
        Boolean prev = processedIds.putIfAbsent(messageId, Boolean.TRUE);
        if (prev != null) return false; // 이미 존재 → 중복

        // 새 ID를 순서 큐에 넣고, 넘치면 가장 오래된 항목 축출
        synchronized (dedupeLock) {
            processedOrder.addLast(messageId);
            if (processedOrder.size() > DEDUPE_WINDOW_SIZE) {
                String old = processedOrder.removeFirst();
                if (old != null) processedIds.remove(old);
            }
        }
        return true;
    }

    /**
     * 순서 위반 검사:
     * - Message에 getKey()/getSequence()가 있을 때만 동작 (없으면 skip)
     * - 현재 seq <= 마지막 seq → 순서 위반 기록
     * - 정상/위반 여부와 관계없이 마지막 seq 갱신(더 큰 값만)
     */
    private void checkOrderViolation(Message msg) {
        String key = msg.getKey();
        Long   seq = msg.getSequence();
        if (key == null || seq == null) return;

        lastSeqByKey.compute(key, (k, prev) -> {
            if (prev != null && seq <= prev) {
                metrics.recordOrderViolation(); // 순서 위반 카운트
                log.warn("[MyMQ-Consumer] 순서 위반 감지 | key={} prev={} curr={}", k, prev, seq);
                // prev 유지(큰 값 유지) 또는 seq로 갱신할지 선택. 일반적으로 더 큰 값 유지:
                return Math.max(prev, seq);
            }
            return (prev == null) ? seq : Math.max(prev, seq);
        });
    }

    @PostConstruct
    void startWorkers() {
        worker = new Thread(this::consumeLoop, "mymq-consumer-1");
        worker.setDaemon(true);
        worker.start();
        log.info("[MyMQ-Consumer] 워커 실행");
    }

    @PreDestroy
    void stopWorkers() throws InterruptedException {
        running = false;
        if (worker != null) {
            worker.interrupt(); // poll 대기/park 해제용
            worker.join(5_000);
        }
        log.info("[MyMQ-Consumer] 워커 정지");
    }

    /** 지속 폴링 워커(각 스레드가 이 메서드를 무한 루프로 수행) */
    private void consumeLoop() {
        // 큐가 비었을 때 잠깐 쉬어주는 대기 시간(ns). 과도한 busy loop 방지.
        final long idleSleepNs = TimeUnit.MILLISECONDS.toNanos(Math.max(1, cfg.getPollIntervalMs())); // 최소 1ms 보장

        while (running) {                                         // 종료 신호가 올 때까지 반복
            try {
                // 브로커에서 메시지 하나를 꺼냄(최대 50ms 대기). 없으면 null.
                Message msg = broker.poll(50);

                if (msg == null) {                                // 큐가 비어있으면
                    LockSupport.parkNanos(idleSleepNs);           // 짧게 쉰 뒤 재폴링
                    continue;                                     // 루프 상단으로
                }

                long now = System.currentTimeMillis();            // 현재 시각
                long latency = Math.max(0, now - msg.getTimestamp()); // E2E 지연(음수 보정)

                boolean decDone = false;                          // 미커밋 카운트 1회만 감소하도록 가드

                try {
                    /* ==== (1) 중복 감지: 이미 처리한 ID면 즉시 드롭 ==== */
                    String msgId = msg.getId();
                    if (!checkAndRemember(msgId)) {
                        metrics.recordDuplicate();   // 중복 카운트
                        metrics.decUncommitted();    // 큐에서 빠졌으므로 미커밋 -1
                        decDone = true;
                        log.warn("[MyMQ-Consumer] 중복 드롭 | id={}", msgId);
                        continue; // 비즈니스 처리 스킵
                    }

                    /* ==== (2) 순서 위반 감지(키/시퀀스 기반) ==== */
                    checkOrderViolation(msg);

                    /* ==== (3) 실제 처리(데모: 로그 + 지표 반영) ==== */
                    log.debug("[MyMQ-Consumer] 처리 | id={} | payload={} | latency={}ms",
                            msg.getId(), msg.getPayload(), latency);

                    metrics.recordMessage(latency);

                    // 기존 전략 유지: 성공 시 멱등 저장소에서 제거(사용처에 따라 의미가 다를 수 있음)
                    idempotencyStore.removeProcessed(msg.getId());

                    metrics.decUncommitted();
                    decDone = true;

                } catch (Exception e) {
                    log.error("[MyMQ-Consumer] 처리 실패 | id={} | 이유={}", msg.getId(), e.getMessage(), e);
                    metrics.recordFailure();
                    metrics.decUncommitted();
                    decDone = true;

                } finally {
                    if (!decDone) {
                        metrics.decUncommitted();
                    }
                }
            } catch (Throwable t) {
                if (!running && t instanceof InterruptedException) break;
                log.error("[MyMQ-Consumer] 워커 예외: {}", t.getMessage(), t);
            }
        }
    }

    public void resetConsistencyWindows() {
        synchronized (dedupeLock) {
            processedOrder.clear();
            processedIds.clear();
        }
        lastSeqByKey.clear();
        log.info("[MyMQ-Consumer] dedupe/order 상태 초기화 완료");
    }
}