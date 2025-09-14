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

import java.util.concurrent.*;
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

    private ExecutorService pool;
    private volatile boolean running = true;

    @PostConstruct
    void startWorkers() {
        int n = Math.max(1, cfg.getNumConsumers());                // 최소 1개 보장
        pool = new ThreadPoolExecutor(
                n,                                                // corePoolSize: 고정 n
                n,                                                // maximumPoolSize: 고정 n
                0L, TimeUnit.MILLISECONDS,                        // keepAlive: 의미 없음(고정 크기)
                new LinkedBlockingQueue<>(),                      // 작업 큐(워커는 루프형이라 큐 거의 사용 안 함)
                new ThreadFactory() {                             // 워커 스레드 커스텀 네이밍
                    final ThreadFactory df = Executors.defaultThreadFactory();
                    @Override public Thread newThread(Runnable r) {
                        Thread t = df.newThread(r);
                        t.setName("mymq-consumer-" + t.getId());  // 스레드명: 문제 추적 용이
                        t.setDaemon(true);                        // 데몬 스레드(프로세스 생명주기와 함께 종료)
                        return t;
                    }
                });

        for (int i = 0; i < n; i++) {
            pool.submit(this::consumeLoop);                       // n개 워커 루프 가동
        }
        log.info("[MyMQ-Consumer] {} worker(s) started.", n);     // 기동 로그
    }

    @PreDestroy
    void stopWorkers() throws InterruptedException {
        running = false;                                          // 워커 루프 종료 신호
        if (pool != null) {
            pool.shutdownNow();                                   // 인터럽트로 깨워 즉시 종료 시도
            pool.awaitTermination(5, TimeUnit.SECONDS);           // 최대 5초 대기
        }
        log.info("[MyMQ-Consumer] workers stopped.");             // 종료 로그
    }

    /** 지속 폴링 워커(각 스레드가 이 메서드를 무한 루프로 수행) */
    private void consumeLoop() {
        // 큐가 비었을 때 잠깐 쉬어주는 대기 시간(ns). 과도한 busy loop 방지.
        final long idleSleepNs = TimeUnit.MILLISECONDS.toNanos(
                Math.max(1, cfg.getPollIntervalMs()));            // 최소 1ms 보장

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
                    // 비즈니스 처리(데모에선 로그 + 지표만 반영)
                    log.debug("[MyMQ-Consumer] 처리 | id={} | payload={} | latency={}ms",
                            msg.getId(), msg.getPayload(), latency);

                    metrics.recordMessage(latency);               // 성공 카운트/지연 통계 반영
                    idempotencyStore.removeProcessed(msg.getId());// 멱등 키 제거(성공 후 삭제 전략)

                    metrics.decUncommitted();                     // 미커밋 -1 (producer의 +1 상쇄)
                    decDone = true;                               // 중복 감소 방지 플래그 세팅

                } catch (Exception e) {
                    // 처리 실패 시
                    log.error("[MyMQ-Consumer] 처리 실패 | id={} | 이유={}", msg.getId(), e.getMessage(), e);
                    metrics.recordFailure();                      // 실패 카운트 반영
                    metrics.decUncommitted();                     // 실패해도 소비는 완료 → 미커밋 -1
                    decDone = true;                               // 중복 감소 방지

                } finally {
                    // 위의 try/catch 블록에서 예외로 빠져 미커밋 감소가 누락됐을 가능성 대비
                    if (!decDone) {
                        metrics.decUncommitted();                 // 최후 방어: 정확히 한 번은 반드시 감소
                    }
                }
            } catch (Throwable t) {                               // 예기치 못한 런타임 예외 방어
                // 워커 스레드가 죽어버리면 처리량 급감 → 루프 유지가 중요
                log.error("[MyMQ-Consumer] 워커 예외: {}", t.getMessage(), t);
                // 루프는 계속 진행. 치명 오류만 아니면 다음 반복에서 복구 시도.
            }
        }
    }
}