package com.realtimefinmq.controller;

import com.realtimefinmq.metrics.MetricsService;
import com.realtimefinmq.metrics.MetricsService.MetricsDto;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST API
 * 실시간 지표 조회용
 */
@RestController
@RequiredArgsConstructor
public class DashboardController {
    private final MetricsService metrics;

    /** 간단한 JSON 지표 (예: {"totalMessages": 42}) */
    @GetMapping("/api/metrics")
    public MetricsDto metrics() {
        return metrics.getMetrics();
    }
}
