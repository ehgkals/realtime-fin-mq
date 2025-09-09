package com.realtimefinmq.metrics;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 메시지 처리 지표 수집 및 출력
 */
@Slf4j
@Service
public class MetricsService {

    // 총 메시지 수 카운트 (AtomicLong 권장: int보다 넓은 범위)
    private final AtomicLong totalMessages = new AtomicLong(0); // 총 처리 시도
    private final AtomicLong successMessages = new AtomicLong(0); // 처리 성공
    private final AtomicLong failMessages = new AtomicLong(0); // 처리 실패

    private final AtomicLong totalLatency = new AtomicLong(0); // 전체 지연 시간 합산
    private final AtomicInteger latencySamples = new AtomicInteger(0); // 표본 수
    private volatile double avgLatencyMs = 0.0; // 평균 지연 시간

    // p95/p99 계산 표본
    private static final int LAT_BUF_SIZE = 10_000; // 최근 1만 건 표본 유지
    private final long[] latencyBuf = new long[LAT_BUF_SIZE];
    private volatile int latencyIdx = 0;
    private volatile boolean latencyFilled = false;

    private final AtomicInteger duplicateCount = new AtomicInteger(0); // 중복 처리
    private final AtomicInteger orderViolationCount = new AtomicInteger(0); // 순서 위반

    private final AtomicInteger uncommittedCount = new AtomicInteger(0); // 커밋 미완료/샐패 수
    private final AtomicInteger dlqCount = new AtomicInteger(0); // DLQ 이동

    private volatile long recoveryTimeMs = 0; // 복구 시간
    private final AtomicInteger recoveredMessages = new AtomicInteger(0); // 복구 후 처리된 메시지 수

    // 메시지 처리 시 호출 → 카운터 1 증가
    public void recordMessage(long latencyMs) {
        // 메시지 처리
        long totalCount = totalMessages.incrementAndGet();
        long successCount = successMessages.incrementAndGet();
        log.info("[Metrics] 총 처리 메시지 수 = {}, 성공 메세지 수 = {}, 실패 메세지 수 = {}", totalCount, successCount, failMessages.get());

        // 평균 지연 시간
        totalLatency.addAndGet(latencyMs);
        int latency = latencySamples.incrementAndGet();
        avgLatencyMs = totalLatency.get() / (double) latency;
        log.debug("[Metrics] 지연 시간 = {}ms, 평균 지연 시간 = {}ms", latencyMs, avgLatencyMs);

        // p95, p99 지연 시간(최근 1만건)
        int idx = latencyIdx % LAT_BUF_SIZE;
        latencyBuf[idx] = latencyMs;
        latencyIdx++;

        if(!latencyFilled && latencyIdx >= LAT_BUF_SIZE) {
            latencyFilled = true;
        }
    }

    // 메세지 처리 실패
    public void recordFailure() {
        totalMessages.incrementAndGet();
        failMessages.incrementAndGet();
        log.warn("[Metrics] 메시지 처리 실패 - 실패 메시지 수 = {}", failMessages);
    }

    // 중복 메세지 기록
    public void recordDuplicate() {
        duplicateCount.incrementAndGet();
        log.warn("[Metrics] 중복 메시지 발생 - 중복 메시지 수 = {}", duplicateCount);
    }

    // 순서 위반 기록
    public void recordOrderViolation() {
        orderViolationCount.incrementAndGet();
        log.warn("[Metrics] 순서 위반 발생 - 순서 위반 수 = {}", orderViolationCount);
    }

    public void recordDlq() {
        dlqCount.incrementAndGet();
        log.warn("[Metrics] 메시지가 DLQ로 이동 - DLQ로 이동한 메시지 수 = {}", dlqCount);
    }

    public void recordRecoveryTime(long ms) {
        this.recoveryTimeMs = ms;
        log.info("[Metrics] Recovery 성공 - 복구 시간 = {}ms", recoveryTimeMs);
    }

    public void recordRecoveryMessage() {
        recoveredMessages.incrementAndGet();
        log.info("[Metrics] 복구된 메세지 수 = {}", recoveredMessages);
    }

    // 현재 지표 스냅샷 반환
    public MetricsDto getMetrics() {
        MetricsDto dto = new MetricsDto();

        dto.setTotalMessages(safeInt(totalMessages.get()));
        dto.setSuccessCount(safeInt(successMessages.get()));
        dto.setFailCount(safeInt(failMessages.get()));

        dto.setAvgLatencyMs(avgLatencyMs);
        Percentiles pp = computePercentiles();
        dto.setP95LatencyMs(pp.p95);
        dto.setP99LatencyMs(pp.p99);

        dto.setDuplicateCount(duplicateCount.get());
        dto.setOrderViolationCount(orderViolationCount.get());

        dto.setUncommitedCount(uncommittedCount.get());
        dto.setDlqCount(dlqCount.get());

        dto.setRecoveryTimeMs(recoveryTimeMs);
        dto.setRecoveredMessages(recoveredMessages.get());

        return dto;
    }

    /** 최근 표본으로 p95/p99 계산 (간단 근사) */
    private Percentiles computePercentiles() {
        int n = Math.min(latencySamples.get(), LAT_BUF_SIZE);

        if (n <= 0)
            return new Percentiles(0.0, 0.0);

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
}
