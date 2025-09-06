package com.realtimefinmq;

import com.realtimefinmq.producer.Producer;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@RequiredArgsConstructor
public class RealtimeFinMqApplication implements CommandLineRunner {
	private final Producer producer;

	public static void main(String[] args) {
		SpringApplication.run(RealtimeFinMqApplication.class, args);
	}

	// 애플리케이션 시작 후 테스트 메시지 전송
	@Override
	public void run(String... args) throws Exception {
		// 테스트용 메시지 10개 전송
		for (int i = 0; i < 10; i++) {
			producer.sendMessage("테스트 메시지 " + i);
		}
	}

}
