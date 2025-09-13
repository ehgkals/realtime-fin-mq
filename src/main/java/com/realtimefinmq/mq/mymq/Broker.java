package com.realtimefinmq.mq.mymq;


import com.realtimefinmq.config.ClusterProps;
import com.realtimefinmq.metrics.BaseMetricsService;
import com.realtimefinmq.metrics.MyMqMetricsService;
import com.realtimefinmq.mq.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.List;


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
    private final Dlq dlq;                // Dead Letter Queue (실패 메시지 저장)
    private final WalAppender wal;        // Write-Ahead Log (메시지 영속화)
    private final IdempotencyStore idem;  // 멱등 저장소 (중복 방지)
    private final MyMqMetricsService metrics; // 지표 집계
    private final ClusterProps cluster;

    private final RestTemplate http = new RestTemplate();

    /**
     * 클라이언트(프로듀서)로부터 들어온 enqueue:
     * 1) 멱등 검사
     * 2) 로컬 WAL
     * 3) 피어 복제 (HTTP POST /_replicate)
     * 4) 쿼럼(ack 수) 확인
     * 5) 로컬 큐 적재 (실패 시 DLQ)
     */
    public boolean enqueue(Message msg) {
        try {
            // 멱등성 검사 (이미 처리한 메시지인지 확인)
            if (idem.alreadyProcessed(msg.getId())) {
                log.warn("[Broker] 중복 메시지 감지 | id={}", msg.getId());
                metrics.recordDuplicate();
                return false;
            }

            // WAL 기록 → 장애 발생 시 복구 가능
            wal.append(msg);

            // 피어 복제
            int acks = 1; // 나 자신 1표
            List<String> peers = cluster.getPeers();
            if (peers != null) {
                for (String url : peers) {
                    try {
                        ResponseEntity<Void> r =
                                http.postForEntity(url + "/_replicate", msg, Void.class);
                        if (r.getStatusCode().is2xxSuccessful()) {
                            acks++;
                        } else {
                            log.warn("[Broker] peer 응답 비정상 peer={} code={}", url, r.getStatusCode());
                        }
                    } catch (Exception e) {
                        log.warn("[Broker] peer 전송 실패 peer={} id={} 이유={}", url, msg.getId(), e.getMessage());
                    }
                }
            }

            // 쿼럼 확인 (설정 기반)
            int needed = effectiveQuorum(peers == null ? 0 : peers.size());
            if (acks < needed) {
                log.error("[Broker] 복제 쿼럼 실패 | id={} acks={} needed={}", msg.getId(), acks, needed);
                dlq.add(msg);              // Dlq.add() 내부에서 DLQ 지표 증가 처리함
                return false;
            }

            // 큐에 메시지 삽입 시도
            boolean ok = queue.offer(msg);

            // 큐가 가득 차면 DLQ로 이동
            if (!ok) {
                log.error("[Broker] 큐 꽉참 → DLQ 이동 | id={}", msg.getId());
                dlq.add(msg);
                return false;
            }

            return true;
        } catch (Exception e) {
            // enqueue 자체 실패 → DLQ로 이동
            log.error("[Broker] enqueue 실패 | id={} | 이유={}", msg.getId(), e.getMessage());
            dlq.add(msg);
            return false;
        }
    }

    /**
     * 피어로부터 들어온 복제 메시지 처리용 엔트리.
     * - 재복제(HTTP 호출) 및 쿼럼 판단은 하지 않는다.
     * - 로컬 WAL 기록 후, 로컬 큐에 적재한다.
     */
    public boolean enqueueFromPeer(Message msg) {
        try {
            // 멱등 체크(중복 방지)
            if (idem.alreadyProcessed(msg.getId())) {
                log.warn("[Broker] (peer) 중복 메시지 감지 | id={}", msg.getId());
                metrics.recordDuplicate();
                return false;
            }

            // 로컬 WAL 기록
            wal.append(msg);

            // 로컬 큐 적재
            boolean ok = queue.offer(msg);
            if (!ok) {
                log.error("[Broker] (peer) 큐 꽉참 → DLQ 이동 | id={}", msg.getId());
                dlq.add(msg);
                metrics.recordDlq();
                return false;
            }

            // 미커밋 +1 (컨슈머가 처리 완료하면 -1)
            metrics.incUncommitted();

            return true;
        } catch (Exception e) {
            log.error("[Broker] (peer) enqueue 실패 | id={} | 이유={}", msg.getId(), e.getMessage(), e);
            dlq.add(msg);
            metrics.recordFailure();
            return false;
        }
    }

    /**
     * 메시지를 큐에서 꺼내오기 (Consumer ← Broker ← Queue)
     *
     * @param timeoutMs poll 대기 시간 (밀리초)
     * @return 큐에서 꺼낸 메시지 (없으면 null)
     */
    public Message poll(long timeoutMs) {
        return queue.poll(timeoutMs);
    }

    /** 설정 기반 쿼럼 계산: 1 ≤ quorum ≤ (자기자신 + 피어수) */
    private int effectiveQuorum(int peerCount) {
        int totalNodes = 1 + peerCount;
        int cfg = cluster.getQuorum(); // application.yml의 cluster.quorum
        if (cfg <= 0) return 1;
        if (cfg > totalNodes) return totalNodes;
        return cfg;
    }
}