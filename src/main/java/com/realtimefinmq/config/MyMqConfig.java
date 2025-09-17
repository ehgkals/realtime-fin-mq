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

    // 소비 주기
    private long pollIntervalMs = 100;
}
