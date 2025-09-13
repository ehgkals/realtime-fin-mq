package com.realtimefinmq.controller;


import com.realtimefinmq.mq.Message;
import com.realtimefinmq.mq.mymq.Broker;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 피어 노드가 보내는 복제 메시지를 수신하는 엔드포인트.
 * - 주의: 수신한 메시지는 "재전파 금지"(loop 방지)
 */
@RestController
@RequiredArgsConstructor
@RequestMapping("/_replicate")
public class ReplicationController {
    private final Broker broker;

    @PostMapping
    public void replicate(@RequestBody Message msg) {
        // 피어에서 온 복제는 로컬에서만 WAL→큐 적재 (재전파 X)
        broker.enqueueFromPeer(msg);
    }
}
