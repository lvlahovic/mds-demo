package com.lvl.mds.orderapi.messaging.consumers;

import com.lvl.mds.orderapi.config.OrderStreamProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

/**
 * Consumes {@code order-results-stream} via the {@code order-service-group}
 * consumer group - the mirror image of what inventory-service does with
 * {@code orders-stream}, and for the same reason: a result published while
 * this service is down has to still be there when it comes back.
 *
 * <p>Same ack discipline as the other direction: {@code XACK} only after the
 * order has been updated. If handling throws, the entry stays in the group's
 * Pending Entries List rather than being lost.
 */
@Component
public class ReservationResultConsumer implements StreamListener<String, MapRecord<String, String, String>> {

	private static final Logger log = LoggerFactory.getLogger(ReservationResultConsumer.class);

	private final ReservationResultProcessor processor;
	private final StreamOperations<String, String, String> streamOps;
	private final String resultStreamKey;
	private final String consumerGroup;

	public ReservationResultConsumer(ReservationResultProcessor processor,
			StringRedisTemplate redisTemplate,
			OrderStreamProperties properties) {
		this.processor = processor;
		this.streamOps = redisTemplate.opsForStream();
		this.resultStreamKey = properties.resultStreamKey();
		this.consumerGroup = properties.resultConsumerGroup();
	}

	@Override
	public void onMessage(MapRecord<String, String, String> record) {
		RecordId recordId = record.getId();
		try {
			processor.process(record);
			streamOps.acknowledge(resultStreamKey, consumerGroup, recordId);
		} catch (Exception ex) {
			log.error("Failed to apply reservation result streamId={} fields={} - leaving unacknowledged",
					recordId, record.getValue(), ex);
		}
	}
}
