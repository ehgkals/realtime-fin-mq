package com.realtimefinmq.mq;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 메시지 DTO
 * Kafka로 주고받을 데이터 구조 정의
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Message {
    private String id;       // 메시지 고유 ID
    private String payload;  // 실제 데이터
    private long timestamp;  // 생성 시각
}