package com.lvl.mds.orderapi.messaging.producers;

import com.lvl.mds.orderapi.config.OrderStreamProperties;
import com.lvl.mds.orderapi.dto.OrderRequestDto;
import com.lvl.mds.orderapi.messaging.event.EventEnvelope;
import com.lvl.mds.orderapi.messaging.event.OrderCreatedPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;

/**
 * Publishes accepted orders to the {@code orders-stream} Redis Stream via
 * {@code XADD}. Redis Streams were chosen over Redis Pub/Sub because Pub/Sub
 * has no persistence or redelivery - a subscriber that is down when a
 * message is published loses it, which does not satisfy the reliable
 * integration requirement of this task. Streams persist entries (with AOF
 * enabled) and let the consumer group replay unacknowledged messages.
 *
 * <p>The order is not written as bare fields but wrapped in an
 * {@link EventEnvelope}, so the consumer sees what kind of event it is and
 * which contract version produced it.
 */
@Component
public class OrderEventPublisher {

	private static final Logger log = LoggerFactory.getLogger(OrderEventPublisher.class);

	private final StringRedisTemplate redisTemplate;
	private final ObjectMapper objectMapper;
	private final String streamKey;

	public OrderEventPublisher(StringRedisTemplate redisTemplate,
			ObjectMapper objectMapper,
			OrderStreamProperties streamProperties) {
		this.redisTemplate = redisTemplate;
		this.objectMapper = objectMapper;
		this.streamKey = streamProperties.streamKey();
	}

	public RecordId publish(OrderRequestDto order) {
		OrderCreatedPayload payload = new OrderCreatedPayload(order.orderId(), order.itemId(), order.quantity());
		EventEnvelope<OrderCreatedPayload> event = EventEnvelope.of(OrderCreatedPayload.EVENT_TYPE, payload);

		Map<String, String> fields = Map.of(EventEnvelope.STREAM_FIELD, objectMapper.writeValueAsString(event));
		RecordId recordId = redisTemplate.opsForStream().add(MapRecord.create(streamKey, fields));

		log.info("Published {} v{} to stream '{}': eventId={}, orderId={}, itemId={}, quantity={}, streamId={}",
				event.eventType(), event.schemaVersion(), streamKey, event.eventId(),
				order.orderId(), order.itemId(), order.quantity(), recordId);

		return recordId;
	}
}
