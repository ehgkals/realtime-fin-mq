package com.realtimefinmq.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "cluster")
public class ClusterProps {
    private String nodeId;
    private List<String> peers;
    private int quorum = 1;
}
