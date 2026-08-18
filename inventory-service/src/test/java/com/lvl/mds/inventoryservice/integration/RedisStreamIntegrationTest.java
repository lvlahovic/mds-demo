package com.lvl.mds.inventoryservice.integration;

import com.lvl.mds.inventoryservice.config.RedisStreamProperties;
import com.lvl.mds.inventoryservice.messaging.StreamListenerLifecycle;
import com.lvl.mds.inventoryservice.messaging.event.EventEnvelope;
import com.lvl.mds.inventoryservice.messaging.event.EventFixtures;
import com.lvl.mds.inventoryservice.messaging.event.ReservationResultPayload;
import com.lvl.mds.inventoryservice.repository.InventoryRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.core.type.TypeReference;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The rest of the suite mocks {@code StreamOperations} - useful for exercising
 * branching logic in isolation, but it can't catch a wrong Redis command, a
 * consumer-group edge case, or an actual crash-and-redelivery. This class
 * boots the real Spring context (consumer group creation, live listener,
 * reclaim job, the lot) against a real {@code redis:8-alpine} container - the
 * same image {@code docker-compose.yml} runs - and drives it only from the
 * outside, through the streams themselves.
 *
 * <p>One Spring context and one Redis container are shared across all three
 * tests (the default Testcontainers/Spring behaviour): each test uses its own
 * {@code orderId} and enough headroom in {@code item-1}'s seeded quantity
 * (100) that reservations made by earlier tests don't affect later
 * assertions.
 */
@Testcontainers
@SpringBootTest
class RedisStreamIntegrationTest {

	@Container
	static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:8-alpine"))
			.withExposedPorts(6379);

	@DynamicPropertySource
	static void redisProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.data.redis.host", redis::getHost);
		registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));

		// Real defaults (30s threshold, 10s scan) would make the crash/redelivery
		// test slow without making it any more correct - only the mechanism is
		// under test here, not the tuning.
		registry.add("inventory.retry.pending-threshold-ms", () -> "500");
		registry.add("inventory.retry.scan-interval-ms", () -> "300");
	}

	private static final TypeReference<EventEnvelope<ReservationResultPayload>> RESULT_EVENT =
			new TypeReference<>() {
			};
	private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(5);

	@Autowired
	private StringRedisTemplate redisTemplate;

	@Autowired
	private RedisStreamProperties streamProperties;

	@Autowired
	private InventoryRepository inventoryRepository;

	@Autowired
	private StreamListenerLifecycle listenerLifecycle;

	@Test
	void reservesStockEndToEndWhenSufficientStock() {
		String orderId = "it-order-" + Instant.now().toEpochMilli();
		int before = inventoryRepository.availableQuantity("item-1");

		streamOps().add(EventFixtures.orderCreated(orderId, "item-1", 3));

		ReservationResultPayload result = awaitResult(orderId);

		assertThat(result.outcome()).isEqualTo("RESERVED");
		assertThat(inventoryRepository.availableQuantity("item-1")).isEqualTo(before - 3);
	}

	@Test
	void rejectsEndToEndWhenStockIsInsufficient() {
		String orderId = "it-order-" + Instant.now().toEpochMilli();

		// item-3 is seeded with only 5 units - always short of this, regardless
		// of what other tests in this class have already reserved.
		streamOps().add(EventFixtures.orderCreated(orderId, "item-3", 999));

		ReservationResultPayload result = awaitResult(orderId);

		assertThat(result.outcome()).isEqualTo("INSUFFICIENT_STOCK");
	}

	/**
	 * Reproduces, deterministically, the manual test already run once by hand
	 * against {@code docker compose} (see CLAUDE.md): a message is read under a
	 * consumer that crashes before acknowledging, so it is left in the group's
	 * Pending Entries List, and {@code PendingMessagesReclaimer} is expected to
	 * claim and successfully reprocess it.
	 *
	 * <p>The live listener is stopped for the width of the "crash" so the test's
	 * own read - standing in for the crashed consumer - is guaranteed to be the
	 * one that claims the message, instead of racing the container's own
	 * consumer for it. Restarting it afterwards is unconditional: a failed
	 * assertion must not leave the context's listener stopped for whichever
	 * test method JUnit runs next.
	 */
	@Test
	void reclaimsAndProcessesAfterASimulatedConsumerCrash() {
		String orderId = "it-order-" + Instant.now().toEpochMilli();
		listenerLifecycle.stop();
		try {
			streamOps().add(EventFixtures.orderCreated(orderId, "item-1", 1));
			readAsGhostConsumerAndNeverAck();
		} finally {
			listenerLifecycle.start();
		}

		ReservationResultPayload result = awaitResult(orderId);

		assertThat(result.outcome()).isEqualTo("RESERVED");
		assertThat(pendingCount()).isZero();
	}

	/**
	 * Stands in for a consumer that crashed mid-processing: reads the one
	 * waiting entry under a consumer name the reclaim job doesn't know about,
	 * then simply never {@code XACK}s it, leaving it in the PEL.
	 */
	private void readAsGhostConsumerAndNeverAck() {
		List<MapRecord<String, String, String>> read = streamOps().read(
				Consumer.from(streamProperties.consumerGroup(), "ghost-consumer"),
				StreamReadOptions.empty().count(1),
				StreamOffset.create(streamProperties.streamKey(), ReadOffset.lastConsumed()));

		assertThat(read).as("ghost consumer should have claimed the message left pending").hasSize(1);
	}

	private long pendingCount() {
		return streamOps().pending(streamProperties.streamKey(), streamProperties.consumerGroup(), Range.unbounded(), 100)
				.size();
	}

	private StreamOperations<String, String, String> streamOps() {
		return redisTemplate.opsForStream();
	}

	/**
	 * Polls {@code order-results-stream} - published to, never consumed from a
	 * group here - until an entry for {@code orderId} shows up, rather than
	 * asserting on a fixed delay.
	 */
	private ReservationResultPayload awaitResult(String orderId) {
		Instant deadline = Instant.now().plus(AWAIT_TIMEOUT);

		while (Instant.now().isBefore(deadline)) {
			Optional<ReservationResultPayload> found = findResult(orderId);
			if (found.isPresent()) {
				return found.get();
			}
			sleep();
		}

		throw new AssertionError("No result for orderId '" + orderId + "' on '" + streamProperties.resultStreamKey()
				+ "' within " + AWAIT_TIMEOUT);
	}

	private Optional<ReservationResultPayload> findResult(String orderId) {
		List<MapRecord<String, String, String>> entries =
				streamOps().range(streamProperties.resultStreamKey(), Range.unbounded());

		for (MapRecord<String, String, String> entry : entries) {
			EventEnvelope<ReservationResultPayload> event =
					EventFixtures.MAPPER.readValue(entry.getValue().get(EventEnvelope.STREAM_FIELD), RESULT_EVENT);
			if (orderId.equals(event.payload().orderId())) {
				return Optional.of(event.payload());
			}
		}
		return Optional.empty();
	}

	private void sleep() {
		try {
			Thread.sleep(100);
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new AssertionError("Interrupted while awaiting a reservation result", ex);
		}
	}
}
