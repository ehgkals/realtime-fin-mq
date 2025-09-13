package com.realtimefinmq.mq.mymq;

import com.realtimefinmq.config.MyMqConfig;
import com.realtimefinmq.mq.Message;
import com.realtimefinmq.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * WalAppender
 * --------------------------
 * WAL (Write-Ahead Log) 구현체
 * - 메시지를 큐(InMemoryQueue)에 넣기 전에 로그 파일에 먼저 기록
 * - 장애 발생 시, 이 로그 파일을 기반으로 메시지를 복구할 수 있음
 *
 * 특징:
 * 1. append() 메서드는 synchronized → 동시 쓰기 충돌 방지
 * 2. 로그 파일은 단순 문자열 기반으로 저장됨
 * 3. 예외 발생 시 SLF4J 로깅을 통해 알림
 */
@Slf4j
@Component
public class WalAppender {
    // 메시지 로그를 기록할 파일 (append 모드)
    private final Path walPath;


    public WalAppender(MyMqConfig cfg) {
        this.walPath = Paths.get(cfg.getWalPath());
        ensureParentDir();
    }

    // 상위 디렉토리 없으면 생성
    private void ensureParentDir() {
        try {
            Path parent = walPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
        } catch (IOException e) {
            log.error("[WAL] 상위 디렉토리 생성 실패 | path={} | 이유={}", walPath, e.getMessage(), e);
        }
    }

    /**
     * 메시지를 WAL 파일에 기록 (append-only)
     * - 장애 발생 시, 이 로그를 기반으로 복구 가능
     * - 매 호출 시 파일을 열고 닫으므로 안정성은 높으나 성능은 떨어질 수 있음
     *
     * @param msg 큐에 넣기 전 기록할 메시지
     */
    public synchronized void append(Message msg) {
        String line = JsonUtils.toJson(msg); // 객체 → JSON 직렬화
        try (BufferedWriter bw = Files.newBufferedWriter(
                walPath, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            bw.write(line);
            bw.newLine();
        } catch (IOException e) {
            log.error("[WAL] 기록 실패 | id={} | path={} | 이유={}", msg.getId(), walPath, e.getMessage(), e);
        }
    }
}
