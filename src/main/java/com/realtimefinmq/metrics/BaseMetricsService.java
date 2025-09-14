package com.realtimefinmq.metrics;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 메시지 처리 지표 수집/집계 서비스
 *
 * - 처리량: total/success/fail
 * - 지연:   평균, p95, p99  (최근 1만 건 표본 기반)
 * - 정합성: duplicate, orderViolation
 * - 내구성: uncommitted(미커밋), dlq
 * - 회복성: recoveryTime, recoveredMessages
 */
@Slf4j
public class BaseMetricsService {

    // ===== 처리량 =====
    private final AtomicLong totalMessages = new AtomicLong(0); // 총 처리 시도
    private final AtomicLong successMessages = new AtomicLong(0); // 처리 성공
    private final AtomicLong failMessages = new AtomicLong(0); // 처리 실패

    // ===== 지연(평균 + 백분위) =====
    private final AtomicLong totalLatency = new AtomicLong(0); // 지연 합
    private final AtomicInteger latencySamples = new AtomicInteger(0); // 표본 수
    private volatile double avgLatencyMs = 0.0; // 평균 지연

    // p95/p99 계산용 순환 버퍼(최근 N건)
    private static final int LAT_BUF_SIZE = 10_000;
    private final long[] latencyBuf = new long[LAT_BUF_SIZE];
    private final AtomicInteger latencyIdx = new AtomicInteger(0); // 다음 기록 인덱스(누적)
    private volatile boolean latencyFilled = false; // 버퍼 한 바퀴 찼는지

    // ===== 정합성 =====
    private final AtomicInteger duplicateCount = new AtomicInteger(0);
    private final AtomicInteger orderViolationCount = new AtomicInteger(0);

    // ===== 내구성 =====
    private final AtomicInteger uncommittedCount = new AtomicInteger(0); // 소비(커밋) 전 메시지 수
    private final AtomicInteger dlqCount = new AtomicInteger(0);

    // ===== 회복성 =====
    private volatile long recoveryTimeMs = 0;
    private final AtomicInteger recoveredMessages = new AtomicInteger(0);

    // ====== 슬라이딩 윈도우용 버퍼 (시간 기반) ======
    // 최근 latency 샘플들의 <발생시각, 지연>을 저장하고 windowMs 이전 것은 제거
    private final Deque<Long> winTs = new ArrayDeque<>();     // epoch millis
    private final Deque<Long> winLat = new ArrayDeque<>();    // latency ms
    // 동시 접근 대비 락 (간단 구현)
    private final Object winLock = new Object();

    // --------------------------------------------------------------------
    // 성공 처리 기록 (컨슈머가 정상 처리했을 때 호출)
    // --------------------------------------------------------------------
    public void recordMessage(long latencyMs) {
        long now = System.currentTimeMillis();
        long total   = totalMessages.incrementAndGet();
        long success = successMessages.incrementAndGet();

        // 누적 평균 지연
        totalLatency.addAndGet(latencyMs);
        int samples = latencySamples.incrementAndGet();
        avgLatencyMs = totalLatency.get() / (double) samples;

        // 백분위용 버퍼에 "지연값"을 저장 (이전 코드에서 샘플 수를 넣던 버그 수정)
        int idx = latencyIdx.getAndIncrement();
        latencyBuf[idx % LAT_BUF_SIZE] = latencyMs;
        if (!latencyFilled && idx + 1 >= LAT_BUF_SIZE) latencyFilled = true;

        // ---- 슬라이딩 윈도우에도 기록 ----
        synchronized (winLock) {
            winTs.addLast(now);
            winLat.addLast(latencyMs);
            // 기본 프루닝은 여기선 하지 않고 getWindowMetrics때 windowMs로 정리
        }

        // 로그는 과도한 출력 방지 위해 debug로
        log.debug("[Metrics] success total={} success={} fail={} latencyMs={} avgMs={}",
                total, success, failMessages.get(), latencyMs, avgLatencyMs);
    }

    // --------------------------------------------------------------------
    // 실패 처리 기록
    // --------------------------------------------------------------------
    public void recordFailure() {
        totalMessages.incrementAndGet();
        int fails = (int) failMessages.incrementAndGet();
        log.warn("[Metrics] 처리 실패 누적={}", fails);
    }

    // --------------------------------------------------------------------
    // 중복 / 순서 위반 / DLQ
    // --------------------------------------------------------------------
    public void recordDuplicate() {
        int d = duplicateCount.incrementAndGet();
        log.warn("[Metrics] 중복 발생 누적={}", d);
    }

    public void recordOrderViolation() {
        int v = orderViolationCount.incrementAndGet();
        log.warn("[Metrics] 순서 위반 누적={}", v);
    }

    public void recordDlq() {
        int d = dlqCount.incrementAndGet();
        log.warn("[Metrics] DLQ 이동 누적={}", d);
    }

    // --------------------------------------------------------------------
    // 미커밋(uncommitted) 증감 (enqueue 성공 시 +1, 소비/커밋 완료 시 -1)
    // --------------------------------------------------------------------
    public void incUncommitted() {
        int u = uncommittedCount.incrementAndGet();
        log.debug("[Metrics] 미커밋 +1 -> {}", u);
    }

    public void decUncommitted() {
        int u = uncommittedCount.decrementAndGet();
        if (u < 0) {
            log.warn("[Metrics] 미커밋 카운터가 0 이하로 내려감 (보정 필요)");
        }
        log.debug("[Metrics] 미커밋 -1 -> {}", u);
    }

    // --------------------------------------------------------------------
    // 회복 관련
    // --------------------------------------------------------------------
    public void recordRecoveryTime(long ms) {
        this.recoveryTimeMs = ms;
        log.info("[Metrics] 복구 시간={}ms", ms);
    }

    public void recordRecoveryMessage() {
        int r = recoveredMessages.incrementAndGet();
        log.info("[Metrics] 복구 후 처리 누적={}", r);
    }

    // --------------------------------------------------------------------
    // 스냅샷 조회 (컨트롤러에서 /api/metrics 응답으로 사용)
    // --------------------------------------------------------------------
    public MetricsDto getMetrics() {
        MetricsDto dto = new MetricsDto();

        dto.setTotalMessages(safeInt(totalMessages.get()));
        dto.setSuccessCount(safeInt(successMessages.get()));
        dto.setFailCount(safeInt(failMessages.get()));
        dto.setAvgLatencyMs(avgLatencyMs);

        Percentiles p = computePercentiles();
        dto.setP95LatencyMs(p.p95);
        dto.setP99LatencyMs(p.p99);

        dto.setDuplicateCount(duplicateCount.get());
        dto.setOrderViolationCount(orderViolationCount.get());
        dto.setUncommittedCount(uncommittedCount.get());
        dto.setDlqCount(dlqCount.get());
        dto.setRecoveryTimeMs(recoveryTimeMs);
        dto.setRecoveredMessages(recoveredMessages.get());

        return dto;
    }

    // 최근 표본으로 p95/p99 계산 (간단 근사)
    private Percentiles computePercentiles() {
        final int n = Math.min(latencySamples.get(), LAT_BUF_SIZE);
        if (n <= 0) return new Percentiles(0.0, 0.0);

        long[] copy = Arrays.copyOf(latencyBuf, n);
        Arrays.sort(copy);

        double p95 = copy[(int) Math.max(0, Math.floor(n * 0.95) - 1)];
        double p99 = copy[(int) Math.max(0, Math.floor(n * 0.99) - 1)];
        return new Percentiles(p95, p99);
    }

    private int safeInt(long v) {
        return (v > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) v;
    }

    private record Percentiles(double p95, double p99) {}

    /**
     * 주어진 windowMs(예: 60_000) 내의 평균/백분위 계산 (그래프용)
     * - 누적값은 건드리지 않음 (상단 요약은 계속 누적)
     */
    public MetricsDto getWindowMetrics(long windowMs) {
        final long limit = System.currentTimeMillis() - Math.max(1, windowMs);

        long[] sample;
        int n = 0;

        synchronized (winLock) {
            // 오래된 샘플 제거
            while (!winTs.isEmpty() && winTs.peekFirst() < limit) {
                winTs.removeFirst();
                winLat.removeFirst();
            }
            // 복사
            n = winLat.size();
            sample = new long[n];
            int i = 0;
            for (long v : winLat) sample[i++] = v;
        }

        MetricsDto dto = new MetricsDto();

        if (n == 0) {
            dto.setAvgLatencyMs(0.0);
            dto.setP95LatencyMs(0.0);
            dto.setP99LatencyMs(0.0);
        } else {
            // 평균
            long sum = 0;
            for (long v : sample) sum += v;
            dto.setAvgLatencyMs(sum / (double) n);

            // p95/p99
            Arrays.sort(sample);
            double p95 = sample[(int)Math.max(0, Math.floor(n * 0.95) - 1)];
            double p99 = sample[(int)Math.max(0, Math.floor(n * 0.99) - 1)];
            dto.setP95LatencyMs(p95);
            dto.setP99LatencyMs(p99);
        }

        // 그래프에서 같이 쓰는 값(즉시성 수치)은 함께 보내주면 편함
        dto.setUncommittedCount(uncommittedCount.get());
        dto.setDlqCount(dlqCount.get());

        return dto;
    }

    // --------------------------------------------------------------------
    // 리셋
    // --------------------------------------------------------------------
    /** 모든 지표(누적 + 윈도우) 초기화 */
    public synchronized void resetAll() {
        // 처리량
        totalMessages.set(0);
        successMessages.set(0);
        failMessages.set(0);

        // 지연
        totalLatency.set(0);
        latencySamples.set(0);
        avgLatencyMs = 0.0;

        Arrays.fill(latencyBuf, 0L);
        latencyIdx.set(0);
        latencyFilled = false;

        // 정합성
        duplicateCount.set(0);
        orderViolationCount.set(0);

        // 내구성
        uncommittedCount.set(0);
        dlqCount.set(0);

        // 회복성
        recoveryTimeMs = 0;
        recoveredMessages.set(0);

        // 윈도우 버퍼도 초기화
        synchronized (winLock) {
            winTs.clear();
            winLat.clear();
        }

        log.info("[Metrics] 모든 지표가 초기화되었습니다.");
    }

    /** 레이턴시 관련(평균/백분위)만 초기화 */
    public synchronized void resetLatencyWindow() {
        totalLatency.set(0);
        latencySamples.set(0);
        avgLatencyMs = 0.0;

        Arrays.fill(latencyBuf, 0L);
        latencyIdx.set(0);
        latencyFilled = false;

        // 윈도우 버퍼도 초기화
        synchronized (winLock) {
            winTs.clear();
            winLat.clear();
        }

        log.info("[Metrics] 레이턴시 윈도우가 초기화되었습니다.");
    }
}