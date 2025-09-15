package com.realtimefinmq;

import com.realtimefinmq.producer.KafkaProducerService;
import com.realtimefinmq.producer.MyMqProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * 애플리케이션 시작 클래스
 * - Spring Boot 실행 진입점
 * - CommandLineRunner를 구현해 애플리케이션 실행 직후 부하 테스트 수행
 * - KafkaProducerService와 MyMqProducerService를 모두 테스트
 */
@Slf4j
@SpringBootApplication
@RequiredArgsConstructor
@ConfigurationPropertiesScan
@EnableScheduling
public class RealtimeFinMqApplication implements CommandLineRunner {
	// Kafka 메시지 발행 서비스
	private final KafkaProducerService kafkaProducerService;

	// MyMQ 메시지 발행 서비스
	private final MyMqProducerService myMqProducerService;

	@Value("${app.autorun:false}") // 기본은 자동실행 안 함
	private boolean autorun;

	public static void main(String[] args) {
		SpringApplication.run(RealtimeFinMqApplication.class, args);
	}

	/**
	 * Spring Boot 실행 직후 호출
	 * - Kafka / MyMQ 각각에 대해 동일 조건으로 부하 테스트 수행
	 */
	@Override
	public void run(String... args) throws Exception {
		if (!autorun) {
			log.info("autorun=false → 부하 테스트 자동 실행 생략");
			return;
		}

		// === 부하 테스트 설정값 ===
		int threads = 4;          // 동시에 실행할 스레드 수
		int totalMessages = 10; // 보낼 메시지 수
		int keyBuckets     = 16;  // MyMQ용 키 버킷(동일 키는 순서 의미)

		// Kafka 테스트
			runLoadTest("Kafka", totalMessages, threads, kafkaProducerService::sendMessage);

		// MyMQ 테스트
		runLoadTestWithKey(
				"MyMQ",
				totalMessages,
				threads,
				(key, payload) -> myMqProducerService.publish(key, payload),
				i -> "key-" + (i % keyBuckets) // 간단한 키 생성 전략
		);
	}

	/**
	 * 공통 부하 테스트 실행 메서드
	 *
	 * @param label          어떤 MQ인지 구분용 (Kafka / MyMQ)
	 * @param totalMessages  총 메시지 수
	 * @param threads        병렬 실행 스레드 개수
	 * @param sender         메시지 발행 메서드 (KafkaProducerService::sendMessage / MyMqProducerService::publish)
	 */
	private void runLoadTest(String label, int totalMessages, int threads,
							 Consumer<String> sender) throws InterruptedException {
		// 고정 크기 스레드 풀 생성 (동시에 threads 개수만큼 작업 수행)
		ExecutorService pool = Executors.newFixedThreadPool(threads);

		// 테스트 시작 시간 기록
		long start = System.currentTimeMillis();

		// totalMessages 개수만큼 메시지 전송
		for (int i = 0; i < totalMessages; i++) {
			final String payload = "부하 테스트 메시지-" + i;
			pool.submit(() -> sender.accept(payload));
		}

		pool.shutdown();
		pool.awaitTermination(10, TimeUnit.MINUTES);

		long took = System.currentTimeMillis() - start;
		long tps = (totalMessages * 1000L) / Math.max(took, 1);

		System.out.println("\n==== [" + label + "] 부하 테스트 결과 ====");
		System.out.println("총 메시지: " + totalMessages);
		System.out.println("소요 시간: " + took + " ms");
		System.out.println("평균 TPS: " + tps);
	}

	/**
	 * 공통 부하 테스트 (key + payload 필요)
	 *
	 * @param keyFn i번째 메시지의 key를 만드는 함수(예: i -> "key-" + (i % 16))
	 */
	private void runLoadTestWithKey(String label,
									int totalMessages,
									int threads,
									BiConsumer<String, String> sender,
									java.util.function.IntFunction<String> keyFn) throws InterruptedException {

		ExecutorService pool = Executors.newFixedThreadPool(threads);
		long start = System.currentTimeMillis();

		for (int i = 0; i < totalMessages; i++) {
			final int idx = i;
			final String payload = "부하 테스트 메시지-" + idx;
			final String key = keyFn.apply(idx);
			pool.submit(() -> sender.accept(key, payload));
		}

		pool.shutdown();
		pool.awaitTermination(10, TimeUnit.MINUTES);

		long took = System.currentTimeMillis() - start;
		long tps = (totalMessages * 1000L) / Math.max(took, 1);

		System.out.println("\n==== [" + label + "] 부하 테스트 결과 ====");
		System.out.println("총 메시지: " + totalMessages);
		System.out.println("소요 시간: " + took + " ms");
		System.out.println("평균 TPS: " + tps);
	}
}