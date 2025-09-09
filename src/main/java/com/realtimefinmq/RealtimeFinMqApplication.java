package com.realtimefinmq;

import com.realtimefinmq.producer.Producer;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
		// 스레드 풀 생성 (병렬 전송)
		int threads = 4;  // 동시에 실행할 스레드 개수
		ExecutorService executor = Executors.newFixedThreadPool(threads);

		int totalMessages = 50_000; // 총 보낼 메시지 수
		long start = System.currentTimeMillis();

		for (int i = 0; i < totalMessages; i++) {
			final int idx = i;
			executor.submit(() -> {
				producer.sendMessage("부하 테스트 메시지 " + idx);
			});
		}

		executor.shutdown();
		executor.awaitTermination(10, TimeUnit.MINUTES);

		long end = System.currentTimeMillis();
		System.out.println("총 " + totalMessages + "개 메시지 전송 완료");
		System.out.println("소요 시간: " + (end - start) + " ms");
		System.out.println("평균 TPS: " + (totalMessages * 1000L) / (end - start));
	}

}
