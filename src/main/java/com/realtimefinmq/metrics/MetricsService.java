package com.realtimefinmq.metrics;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 메시지 처리 지표 수집 및 출력
 */
@Slf4j
@Service
public class MetricsService {

    // 총 메시지 수 카운트 (AtomicLong 권장: int보다 넓은 범위)
    private final AtomicLong totalMessages = new AtomicLong(0);

    /**
     * 메시지 처리 시 호출 → 카운터 1 증가
     */
    public void recordMessage() {
        long count = totalMessages.incrementAndGet();
        log.info("[Metrics] 총 처리 메시지 수 = {}", count);
    }

    /**
     * 현재 지표 스냅샷 반환
     */
    public MetricsDto getMetrics() {
        return new MetricsDto(totalMessages.get());
    }

    /**
     * 불변 DTO (getter만 제공)
     */
    @Getter
    @AllArgsConstructor
    public static class MetricsDto {
        private final long totalMessages;
    }
}
