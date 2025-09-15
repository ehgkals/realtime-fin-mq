package com.realtimefinmq.config;

import com.realtimefinmq.mq.mymq.*;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * MyMQ 관련 설정 클래스
 * - 스프링이 자동으로 Bean을 등록하고, 의존성을 주입할 수 있도록 구성
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "custom-mq")
public class MyMqConfig {
    // InMemoryQueue 용량
    private int queueSize = 10000;

    // DLQ 용량
    private int dlqSize = 1000;

    // 소비 주기 (스케줄러 fixedDelay)
    private long pollIntervalMs = 100;

    // WAL 파일 경로
    private String walPath = "./mymq-wal.log";

    // 병렬 소비자 스레드 수
    private int numConsumers = 1;
}
