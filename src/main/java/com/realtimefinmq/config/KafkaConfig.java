package com.realtimefinmq.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka 관련 설정을 담당하는 클래스
 * - ProducerFactory: KafkaProducer 생성 및 설정
 * - KafkaTemplate: 메시지 전송을 위한 템플릿
 * - ConsumerFactory: KafkaConsumer 생성 및 설정
 * - ConcurrentKafkaListenerContainerFactory: @KafkaListener 동시 처리 설정
 * - NewTopic: 테스트용 토픽 생성
 */
@EnableKafka
@Configuration
public class KafkaConfig {

    // Kafka 브로커 주소 (로컬)
    private final String bootstrapServers = "127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094";

    /**
     * Kafka ProducerFactory 설정
     * 메시지를 Kafka로 보내기 위해 Producer 생성
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        // Kafka 서버 주소 설정
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // 메시지 Key 직렬화 방식
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // 메시지 Value 직렬화 방식
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // acks: 메시지가 브로커에 완전히 기록될 때까지 대기
        // 'all' -> ISR(동기 복제 브로커)에 모두 기록될 때까지 기다림
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");

        // 멱등 프로듀서 활성화
        // 같은 메시지가 중복 전송되더라도 1회만 처리됨
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // ProducerFactory 생성 후 반환
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * KafkaTemplate 생성
     * 메시지를 Kafka 토픽으로 전송할 때 사용
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        // ProducerFactory를 기반으로 KafkaTemplate 생성
        return new KafkaTemplate<>(producerFactory());
    }

    /**
     * Kafka ConsumerFactory 설정
     * Kafka에서 메시지를 읽어오는 Consumer 생성
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();

        // Kafka 서버 주소 설정
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // 메시지 Key 역직렬화 방식
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class);

        // 메시지 Value 역직렬화 방식
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class);

        // 자동 커밋 여부
        // false -> 메시지 처리 후 수동 커밋 가능 (안정성 확보)
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // - "earliest" → 토픽의 가장 처음 메시지부터 읽음
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // DefaultKafkaConsumerFactory 생성 후 반환
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Kafka Listener Container Factory 설정
     * @KafkaListener 동작 시 사용할 Listener Factory
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory
    ) {
        // Listener Factory 생성
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        // ConsumerFactory 설정
        factory.setConsumerFactory(consumerFactory);

        // 동시에 몇 개의 스레드가 메시지 처리할지 설정
        // 3 -> 최대 3개의 스레드가 동시에 메시지 처리
        factory.setConcurrency(3);

        // Poll 시 타임아웃 설정 (ms)
        factory.getContainerProperties().setPollTimeout(3000);

        return factory;
    }
}