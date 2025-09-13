package com.realtimefinmq.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "kafka")
public class KafkaProps {

    private String bootstrapServers;  // kafka.bootstrap-servers
    private String topic;             // kafka.topic

    private Producer producer = new Producer();
    private Consumer consumer = new Consumer();

    @Getter @Setter
    public static class Producer {
        private String acks;
        private Integer retries;
        private Integer batchSize;
        private Integer lingerMs;
        private Long bufferMemory;
        private String keySerializer;
        private String valueSerializer;
        private Boolean enableIdempotence;
    }

    @Getter @Setter
    public static class Consumer {
        private String groupId;
        private Boolean enableAutoCommit;
        private String keyDeserializer;
        private String valueDeserializer;
        private Integer maxPollRecords;
        private String autoOffsetReset; // "earliest" 등 (원하면 사용)
    }
}