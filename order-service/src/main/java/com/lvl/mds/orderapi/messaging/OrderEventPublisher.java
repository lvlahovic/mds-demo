package com.lvl.mds.orderapi.messaging;

import com.lvl.mds.orderapi.config.OrderStreamProperties;
import com.lvl.mds.orderapi.dto.OrderRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Publishes accepted orders to the {@code orders-stream} Redis Stream via
 * {@code XADD}. Redis Streams were chosen over Redis Pub/Sub because Pub/Sub
 * has no persistence or redelivery - a subscriber that is down when a
 * message is published loses it, which does not satisfy the reliable
 * integration requirement of this task. Streams persist entries (with AOF
 * enabled) and let the consumer group replay unacknowledged messages.
 */
@Component
public class OrderEventPublisher {

	private static final Logger log = LoggerFactory.getLogger(OrderEventPublisher.class);

	private final StringRedisTemplate redisTemplate;
	private final String streamKey;

	public OrderEventPublisher(StringRedisTemplate redisTemplate, OrderStreamProperties streamProperties) {
		this.redisTemplate = redisTemplate;
		this.streamKey = streamProperties.streamKey();
	}

	public RecordId publish(OrderRequest order) {
		Map<String, String> fields = new LinkedHashMap<>();
		fields.put("orderId", order.orderId());
		fields.put("itemId", order.itemId());
		fields.put("quantity", String.valueOf(order.quantity()));

		MapRecord<String, String, String> record = MapRecord.create(streamKey, fields);
		RecordId recordId = redisTemplate.opsForStream().add(record);

		log.info("Published order event to stream '{}': orderId={}, itemId={}, quantity={}, streamId={}",
				streamKey, order.orderId(), order.itemId(), order.quantity(), recordId);

		return recordId;
	}
}
