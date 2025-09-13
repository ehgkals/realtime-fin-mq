package com.realtimefinmq.mq.mymq;

import com.realtimefinmq.metrics.MyMqMetricsService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * IdempotencyStore
 * --------------------------
 * 멱등성(Idempotency) 보장 컴포넌트
 *
 * 배경:
 * - 메시지 큐 환경에서는 네트워크 장애, 재시도 로직 등으로
 *   같은 메시지가 두 번 이상 도착할 수 있음
 * - 같은 메시지를 중복 처리하면 금액이 두 번 결제되는 등의 문제 발생 가능
 *
 * 역할:
 * 1. 이미 처리한 메시지 ID를 저장
 * 2. 새 메시지의 ID를 확인 → 이미 처리했다면 "중복"으로 판별
 *
 * 구현 방식:
 * - ConcurrentHashMap.newKeySet() → 멀티스레드 환경에서도 안전
 * - add() 성공 시 → 처음 들어온 메시지 (false 반환)
 * - add() 실패 시 → 이미 존재하는 메시지 (true 반환 → 중복)
 */
@Component
@RequiredArgsConstructor
public class IdempotencyStore {
    // 처리된 메시지 ID를 저장하는 집합
    private final Set<String> processedIds = ConcurrentHashMap.newKeySet();
    private final MyMqMetricsService metrics;

    /**
     * 메시지가 이미 처리된 적 있는지 확인
     *
     * @param id 메시지 고유 ID
     * @return true → 이미 처리됨(중복)
     *         false → 처음 처리됨
     */
    public boolean alreadyProcessed(String id) {
        boolean duplicate = !processedIds.add(id);
        if (duplicate && metrics != null) {
            metrics.recordDuplicate();
        }
        return duplicate; // 이미 있으면 true
    }

    /**
     * 처리 완료(커밋) 후, 멱등 저장소에서 ID를 제거한다.
     * - 실험/재처리를 위해 동일 ID의 재수용을 허용하고 싶을 때 사용
     * - 운영에서 "영구 멱등"이 필요하면 제거하지 않는 전략 사용
     *
     * @param id 메시지 고유 ID
     * @return true  → 이미 처리됨(중복)
     *         false → 처음 처리됨
     */
    public boolean removeProcessed(String id) {
        return processedIds.remove(id);
    }

    /**
     * 테스트/리셋용: 모든 기록 초기화
     */
    public void clear() {
        processedIds.clear();
    }
}