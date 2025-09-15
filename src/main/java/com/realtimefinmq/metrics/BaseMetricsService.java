package com.realtimefinmq.metrics;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 메시지 처리 지표 (간소화)
 * - 처리량: total / success / fail
 * - 지연:   avg / p95 / p99  (최근 N건 표본 + 시간 윈도우)
 * - 정합성: duplicate / orderViolation
 * - 내구성(간이): uncommitted (프로듀서 +1, 컨슈머 -1)
 */
@Slf4j
public class BaseMetricsService {

    // ===== 처리량 =====
    private final AtomicLong totalMessages   = new AtomicLong(0);
    private final AtomicLong successMessages = new AtomicLong(0);
    private final AtomicLong failMessages    = new AtomicLong(0);

    // ===== 지연(평균 + 백분위) =====
    private final AtomicLong   totalLatency    = new AtomicLong(0);
    private final AtomicInteger latencySamples = new AtomicInteger(0);
    private volatile double    avgLatencyMs    = 0.0;

    private static final int LAT_BUF_SIZE = 10_000;
    private final long[] latencyBuf = new long[LAT_BUF_SIZE];
    private final AtomicInteger latencyIdx = new AtomicInteger(0);

    // ===== 정합성 =====
    private final AtomicInteger duplicateCount      = new AtomicInteger(0);
    private final AtomicInteger orderViolationCount = new AtomicInteger(0);

    // ===== 내구성(간이) =====
    private final AtomicInteger uncommittedCount = new AtomicInteger(0);

    // ===== 시간 기반 슬라이딩 윈도우(차트용) =====
    private final Deque<Long> winTs  = new ArrayDeque<>(); // epoch millis
    private final Deque<Long> winLat = new ArrayDeque<>(); // latency ms
    private final Object winLock = new Object();

    // --------------------------------------------------------------------
    // 기록 API
    // --------------------------------------------------------------------
    public void recordMessage(long latencyMs) {
        long now = System.currentTimeMillis();

        totalMessages.incrementAndGet();
        successMessages.incrementAndGet();

        totalLatency.addAndGet(latencyMs);
        int samples = latencySamples.incrementAndGet();
        avgLatencyMs = totalLatency.get() / (double) samples;

        int idx = latencyIdx.getAndIncrement();
        latencyBuf[idx % LAT_BUF_SIZE] = latencyMs;

        synchronized (winLock) {
            winTs.addLast(now);
            winLat.addLast(latencyMs);
        }

        log.debug("[Metrics] success latencyMs={} avgMs={}", latencyMs, avgLatencyMs);
    }

    public void recordFailure() {
        totalMessages.incrementAndGet();
        failMessages.incrementAndGet();
    }

    public void recordDuplicate() {
        duplicateCount.incrementAndGet();
    }

    public void recordOrderViolation() {
        orderViolationCount.incrementAndGet();
    }

    // 언커밋 증감 (프로듀서 enqueue 성공 시 +1, 컨슈머 처리/실패 시 -1)
    public void incUncommitted() {
        uncommittedCount.incrementAndGet();
    }
    public void decUncommitted() {
        int u = uncommittedCount.decrementAndGet();
        if (u < 0) {
            // 음수 방지용 경고(레이스/보정 확인용)
            log.warn("[Metrics] uncommitted < 0 (보정 필요)");
        }
    }

    // --------------------------------------------------------------------
    // 누적 스냅샷
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
        return dto;
    }

    private Percentiles computePercentiles() {
        final int n = Math.min(latencySamples.get(), LAT_BUF_SIZE);
        if (n <= 0) return new Percentiles(0.0, 0.0);
        long[] copy = Arrays.copyOf(latencyBuf, n);
        Arrays.sort(copy);
        double p95 = copy[(int)Math.max(0, Math.floor(n * 0.95) - 1)];
        double p99 = copy[(int)Math.max(0, Math.floor(n * 0.99) - 1)];
        return new Percentiles(p95, p99);
    }

    // --------------------------------------------------------------------
    // 윈도우 스냅샷 (차트)
    // --------------------------------------------------------------------
    public MetricsDto getWindowMetrics(long windowMs) {
        final long limit = System.currentTimeMillis() - Math.max(1, windowMs);

        long[] sample;
        int n;

        synchronized (winLock) {
            while (!winTs.isEmpty() && winTs.peekFirst() < limit) {
                winTs.removeFirst();
                winLat.removeFirst();
            }
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
            long sum = 0;
            for (long v : sample) sum += v;
            dto.setAvgLatencyMs(sum / (double) n);

            Arrays.sort(sample);
            double p95 = sample[(int)Math.max(0, Math.floor(n * 0.95) - 1)];
            double p99 = sample[(int)Math.max(0, Math.floor(n * 0.99) - 1)];
            dto.setP95LatencyMs(p95);
            dto.setP99LatencyMs(p99);
        }
        // 그래프용 실시간 수치
        dto.setUncommittedCount(uncommittedCount.get());
        return dto;
    }

    // --------------------------------------------------------------------
    // 리셋
    // --------------------------------------------------------------------
    public synchronized void resetAll() {
        totalMessages.set(0);
        successMessages.set(0);
        failMessages.set(0);

        totalLatency.set(0);
        latencySamples.set(0);
        avgLatencyMs = 0.0;
        Arrays.fill(latencyBuf, 0L);
        latencyIdx.set(0);

        duplicateCount.set(0);
        orderViolationCount.set(0);
        uncommittedCount.set(0);

        synchronized (winLock) {
            winTs.clear();
            winLat.clear();
        }
    }

    public synchronized void resetLatencyWindow() {
        totalLatency.set(0);
        latencySamples.set(0);
        avgLatencyMs = 0.0;
        Arrays.fill(latencyBuf, 0L);
        latencyIdx.set(0);

        synchronized (winLock) {
            winTs.clear();
            winLat.clear();
        }
    }

    private int safeInt(long v) {
        return (v > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) v;
    }
    private record Percentiles(double p95, double p99) {}
}
