package com.lvl.mds.inventoryservice.messaging.consumers;

import com.lvl.mds.inventoryservice.config.RedisStreamProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

/**
 * Consumes {@code orders-stream} via the {@code inventory-service-group}
 * consumer group and acknowledges ({@code XACK}) only after the order has
 * been successfully handled. If processing throws, the entry is
 * deliberately left unacknowledged - it stays in the group's Pending
 * Entries List and is picked back up by {@link PendingMessagesReclaimer}.
 */
@Component
public class OrderEventConsumer implements StreamListener<String, MapRecord<String, String, String>> {

	private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

	private final OrderEventProcessor processor;
	private final StreamOperations<String, String, String> streamOps;
	private final String streamKey;
	private final String consumerGroup;

	public OrderEventConsumer(OrderEventProcessor processor,
			StringRedisTemplate redisTemplate,
			RedisStreamProperties properties) {
		this.processor = processor;
		this.streamOps = redisTemplate.opsForStream();
		this.streamKey = properties.streamKey();
		this.consumerGroup = properties.consumerGroup();
	}

	@Override
	public void onMessage(MapRecord<String, String, String> record) {
		RecordId recordId = record.getId();
		try {
			processor.process(record);
			streamOps.acknowledge(streamKey, consumerGroup, recordId);
		} catch (Exception ex) {
			log.error("Failed to process order event streamId={} fields={} - leaving unacknowledged for retry",
					recordId, record.getValue(), ex);
		}
	}
}
