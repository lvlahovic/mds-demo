package com.lvl.mds.inventoryservice.messaging;

import com.lvl.mds.inventoryservice.config.RedisStreamProperties;
import com.lvl.mds.inventoryservice.config.RetryProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reliability net for the consumer group: periodically inspects
 * {@code XPENDING} for entries that have sat unacknowledged longer than
 * {@code inventory.retry.pending-threshold-ms} - e.g. because the consumer
 * crashed mid-processing - claims them ({@code XCLAIM}) and retries them
 * through the same {@link OrderEventProcessor} path the live consumer uses.
 * Once a message's delivery count exceeds {@code inventory.retry.max-attempts}
 * it is written to {@code orders-stream-dlq} and acknowledged off the
 * original stream instead of being retried again.
 */
@Component
public class PendingMessagesReclaimer {

	private static final Logger log = LoggerFactory.getLogger(PendingMessagesReclaimer.class);

	private final StreamOperations<String, String, String> streamOps;
	private final OrderEventProcessor processor;
	private final RedisStreamProperties streamProperties;
	private final RetryProperties retryProperties;

	public PendingMessagesReclaimer(StringRedisTemplate redisTemplate,
			OrderEventProcessor processor,
			RedisStreamProperties streamProperties,
			RetryProperties retryProperties) {
		this.streamOps = redisTemplate.opsForStream();
		this.processor = processor;
		this.streamProperties = streamProperties;
		this.retryProperties = retryProperties;
	}

	@Scheduled(fixedDelayString = "${inventory.retry.scan-interval-ms}")
	public void reclaimStalePendingMessages() {
		PendingMessages pending;
		try {
			pending = streamOps.pending(streamProperties.streamKey(), streamProperties.consumerGroup(), Range.unbounded(), 100);
		} catch (Exception ex) {
			// e.g. the stream/group doesn't exist yet - nothing to reclaim.
			log.debug("Skipping pending-message scan: {}", ex.getMessage());
			return;
		}

		Duration threshold = Duration.ofMillis(retryProperties.pendingThresholdMs());

		for (PendingMessage message : pending) {
			if (message.getElapsedTimeSinceLastDelivery().compareTo(threshold) >= 0) {
				handleStaleMessage(message, threshold);
			}
		}
	}

	private void handleStaleMessage(PendingMessage message, Duration minIdleTime) {
		RecordId id = message.getId();
		long deliveryCount = message.getTotalDeliveryCount();

		List<MapRecord<String, String, String>> claimed = streamOps.claim(
				streamProperties.streamKey(), streamProperties.consumerGroup(), streamProperties.consumerName(),
				minIdleTime, id);

		if (claimed.isEmpty()) {
			// Already claimed/acked by another scan or consumer in the meantime.
			return;
		}
		MapRecord<String, String, String> record = claimed.get(0);

		if (deliveryCount > retryProperties.maxAttempts()) {
			moveToDlq(record);
			return;
		}

		try {
			processor.process(record);
			streamOps.acknowledge(streamProperties.streamKey(), streamProperties.consumerGroup(), id);
			log.info("Reclaimed and reprocessed stale message streamId={} (delivery #{})", id, deliveryCount + 1);
		} catch (Exception ex) {
			log.warn("Reclaimed message streamId={} failed again (delivery #{}) - remains pending for next scan",
					id, deliveryCount + 1, ex);
		}
	}

	private void moveToDlq(MapRecord<String, String, String> record) {
		Map<String, String> dlqFields = new LinkedHashMap<>(record.getValue());
		dlqFields.put("originalStreamId", record.getId().getValue());
		dlqFields.put("failureReason", "exceeded max delivery attempts (" + retryProperties.maxAttempts() + ")");

		streamOps.add(MapRecord.create(streamProperties.dlqStreamKey(), dlqFields));
		streamOps.acknowledge(streamProperties.streamKey(), streamProperties.consumerGroup(), record.getId());

		log.error("Order permanently failed after exceeding retry attempts - moved to DLQ '{}': streamId={}, fields={}",
				streamProperties.dlqStreamKey(), record.getId(), record.getValue());
	}
}
