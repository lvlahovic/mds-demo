package com.lvl.mds.orderapi.messaging;

import com.lvl.mds.orderapi.config.OrderStreamProperties;
import com.lvl.mds.orderapi.messaging.consumers.ReservationResultProcessor;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Brings the result-stream consumer to a known state before it starts
 * polling: creates the {@code order-service-group} consumer group, then
 * drains anything this consumer left unacknowledged in a previous life.
 *
 * <p>Creating the group at startup means order-service can come up before
 * inventory-service has ever published a result (Spring Data Redis issues
 * {@code XGROUP CREATE ... MKSTREAM}). Running it on every startup is safe -
 * an existing group just raises BUSYGROUP, which is caught and ignored.
 *
 * <p>The drain matters because the live listener reads with the "new
 * messages only" offset, which never returns entries already delivered to
 * the group - anything delivered but not acknowledged (this service died
 * mid-update) would sit in the Pending Entries List forever, growing Redis
 * memory and never being retried. Reading from offset {@code 0} returns
 * exactly this consumer's own pending entries. With state kept in memory,
 * most drained results will be for orders that didn't survive the restart,
 * and are simply logged and acknowledged; the moment orders are persisted,
 * this same code becomes a real recovery path.
 */
@Component
@ConditionalOnProperty(prefix = "order.redis", name = "result-listener-enabled", havingValue = "true", matchIfMissing = true)
public class ResultStreamInitializer {

	private static final int DRAIN_BATCH_SIZE = 100;

	private static final Logger log = LoggerFactory.getLogger(ResultStreamInitializer.class);

	private final StreamOperations<String, String, String> streamOps;
	private final ReservationResultProcessor processor;
	private final OrderStreamProperties properties;

	public ResultStreamInitializer(StringRedisTemplate redisTemplate,
			ReservationResultProcessor processor,
			OrderStreamProperties properties) {
		this.streamOps = redisTemplate.opsForStream();
		this.processor = processor;
		this.properties = properties;
	}

	@PostConstruct
	public void prepareResultStream() {
		createConsumerGroup();
		drainOwnPendingEntries();
	}

	private void createConsumerGroup() {
		try {
			streamOps.createGroup(properties.resultStreamKey(), ReadOffset.from("0"), properties.resultConsumerGroup());
			log.info("Created consumer group '{}' on stream '{}'",
					properties.resultConsumerGroup(), properties.resultStreamKey());
		} catch (DataAccessException ex) {
			if (isBusyGroup(ex)) {
				log.info("Consumer group '{}' already exists on stream '{}'",
						properties.resultConsumerGroup(), properties.resultStreamKey());
			} else {
				throw ex;
			}
		}
	}

	private void drainOwnPendingEntries() {
		int drained = 0;

		// Each entry is acknowledged as it is handled, so the next read comes
		// back with whatever is left - an empty batch means the PEL is clear.
		List<MapRecord<String, String, String>> batch = readPendingBatch();
		while (!batch.isEmpty()) {
			for (MapRecord<String, String, String> record : batch) {
				processor.process(record);
				streamOps.acknowledge(properties.resultStreamKey(), properties.resultConsumerGroup(), record.getId());
				drained++;
			}
			batch = readPendingBatch();
		}

		if (drained > 0) {
			log.info("Drained {} reservation result(s) left pending by a previous run", drained);
		}
	}

	/**
	 * Reads the next batch of this consumer's own pending entries. Offset
	 * {@code 0} is what makes it "already delivered to me but never
	 * acknowledged" rather than "new to the group".
	 */
	private List<MapRecord<String, String, String>> readPendingBatch() {
		Consumer consumer = Consumer.from(properties.resultConsumerGroup(), properties.resultConsumerName());
		List<MapRecord<String, String, String>> batch = streamOps.read(consumer,
				StreamReadOptions.empty().count(DRAIN_BATCH_SIZE),
				StreamOffset.create(properties.resultStreamKey(), ReadOffset.from("0")));

		return batch == null ? List.of() : batch;
	}

	/**
	 * Spring Data Redis wraps the driver exception (e.g.
	 * {@code RedisSystemException}), whose own message is a generic
	 * "Error in execution" - the actual {@code BUSYGROUP} text from Redis is
	 * on the cause. Walk the chain instead of checking {@code ex.getMessage()}
	 * alone.
	 */
	private boolean isBusyGroup(Throwable ex) {
		for (Throwable current = ex; current != null; current = current.getCause()) {
			if (current.getMessage() != null && current.getMessage().contains("BUSYGROUP")) {
				return true;
			}
		}
		return false;
	}
}
