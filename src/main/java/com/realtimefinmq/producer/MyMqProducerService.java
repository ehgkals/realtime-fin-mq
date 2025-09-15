package com.realtimefinmq.producer;

import com.realtimefinmq.metrics.MyMqMetricsService;
import com.realtimefinmq.mq.Message;              // 공용 메시지 DTO(id, payload, ts)
import com.realtimefinmq.mq.mymq.Broker;     // MyMQ 브로커(큐/WAL/멱등/DLQ 오케스트레이션)
import lombok.RequiredArgsConstructor;           // 생성자 주입(@RequiredArgsConstructor)
import lombok.extern.slf4j.Slf4j;                // 로그 사용용(@Slf4j)
import org.springframework.stereotype.Service;   // 스프링 빈 등록(@Service)

import java.util.UUID;                           // 메시지 ID 생성용
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MyMQ로 메시지를 발행(Enqueue)하는 프로듀서.
 *
 * 책임
 * - 외부에서 받은 payload로 Message를 구성(id/ts 부여)
 * - Broker.enqueue(...) 호출로 큐에 적재
 * - (선택) 지표: enqueue 성공 시 미커밋 카운트 증가
 *
 * 주의
 * - 부하 테스트에서는 per-message INFO 로그가 시스템을 방해하므로 DEBUG에 두는 것을 권장
 * - 예외는 로그로 남기되 호출자에게 성공/실패를 boolean으로 알려주면 상위에서 집계 가능
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MyMqProducerService {

    private final Broker broker;
    private final MyMqMetricsService metrics;
    private final ConcurrentMap<String, AtomicLong> seqByKey = new ConcurrentHashMap<>(); // 시퀀스 저장소


    /** payload로부터 기본 키를 유도 (동일 payload → 동일 키가 되도록) */
    private String deriveKey(String payload) {
        if (payload == null || payload.isBlank()) return "key-default";
        int bucket = Math.abs(payload.hashCode()) % 16; // 0~15
        return "key-" + bucket;
    }

    /** 키별 다음 시퀀스 */
    private long nextSeq(String key) {
        return seqByKey.computeIfAbsent(key, k -> new AtomicLong(0)).incrementAndGet();
    }

    /**
     * 단일 메시지 발행 (UUID 자동 생성).
     *
     * @param payload 전송할 데이터
     * @return true: enqueue 성공(큐에 들어감) / false: 중복·용량초과·예외 등으로 미수용
     */
    public boolean publish(String key, String payload) {
        // null 또는 비어있는 payload 스킵
        if (payload == null || payload.isBlank()) {
            log.warn("[MyMQ-Producer] 비어있는 payload 스킵");
            return false;
        }
        if (key == null || key.isBlank()) {
            key = "key-default";
        }

        // 메세지 생성
        final String id = UUID.randomUUID().toString();     // 메시지 ID
        final long ts = System.currentTimeMillis();         // 전송 타임스탬프
        final long seq = nextSeq(key);                      // 시퀀스
        final Message msg = new Message(id, payload, ts, key, seq);   // 공용 DTO

        log.debug("[MyMQ-Producer] 생성 | id={} key={} seq={} ts={} payload={}", id, key, seq, ts, payload);

        try {
            // 브로커 enqueue (내부에서 WAL/멱등/DLQ 처리)
            final boolean accepted = broker.enqueue(msg);

            // 결과 로그
            if (!accepted) {
                // 큐 포화, 중복 등으로 정상 큐에 못들어간 케이스 (DLQ로 갔을 수 있음)
                log.warn("[MyMQ-Producer] enqueue 실패/우회(DLQ 가능성) | id={} key={} seq={}", id, key, seq);
                return false;
            } else {
                log.debug("[MyMQ-Producer] 메시지 발행 완료 | id={} key={} seq={}", id, key, seq);
                metrics.incUncommitted(); // 언커밋 +1
                return true;
            }
        } catch (Exception e) {
            // enqueue 중 예외 (WAL 실패 / 디스크 오류 / 장애주입 등)
            log.error("[MyMQ-Producer] 메시지 발행 실패 | id={} key={} seq={} | 이유={}", id, key, seq, e.getMessage(), e);
            return false;
        }
    }
}