package com.realtimefinmq.metrics;

import lombok.Data;

/**
 * 지표 데이터 DTO
 */
@Data
public class MetricsDto {
    // 처리량
    private int totalMessages; // 총 메시지 수
    private int successCount; // 처리 성공 메시지 수
    private int failCount; // 처리 실패 메시지 수

    // 지연
    private double avgLatencyMs; // 평균 처리 지연 시간
    private double p95LatencyMs; // 95% 구간 지연 시간
    private double p99LatencyMs; // 99% 구간 지연 시간

    // 정합성
    private int duplicateCount; // 중복 처리된 메시지 수
    private int orderViolationCount; // 순서 위반 발생 건수

    // 내구성
    private int uncommittedCount; // 커밋되지 않은 메시지 수
}
