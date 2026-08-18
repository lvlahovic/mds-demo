package com.lvl.mds.inventoryservice.messaging.consumers;

import com.lvl.mds.inventoryservice.messaging.event.EventEnvelope;
import com.lvl.mds.inventoryservice.messaging.event.OrderCreatedPayload;
import com.lvl.mds.inventoryservice.messaging.producers.ReservationResultPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.util.Optional;

/**
 * Everything that knows how an order event is encoded on the stream, in one
 * place - both the normal processing path ({@link OrderEventProcessor}) and
 * the give-up path ({@link ReservationResultPublisher#publishFailure}) have
 * to decode the same entry, and they must not drift apart.
 */
@Component
public class OrderEventReader {

	private static final TypeReference<EventEnvelope<OrderCreatedPayload>> ORDER_EVENT =
			new TypeReference<>() {
			};

	private static final Logger log = LoggerFactory.getLogger(OrderEventReader.class);

	private final ObjectMapper objectMapper;

	public OrderEventReader(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	/**
	 * Decodes an entry, rejecting anything this build has no business
	 * interpreting. Throwing is the right answer on the live path: the caller
	 * leaves the entry unacknowledged, so it is retried and eventually
	 * dead-lettered rather than silently dropped.
	 */
	public EventEnvelope<OrderCreatedPayload> read(MapRecord<String, String, String> record) {
		EventEnvelope<OrderCreatedPayload> event = parse(record);
		event.requireSupportedContract(OrderCreatedPayload.EVENT_TYPE);

		return event;
	}

	/**
	 * Best-effort decode for the DLQ path, where the reason the message is
	 * being given up on can be that it doesn't decode, or that it decodes but
	 * carries a contract this build refuses ({@link #read} would throw either
	 * way). Deliberately skips {@link EventEnvelope#requireSupportedContract}:
	 * a message dead-lettered for exactly that reason must still be nameable
	 * in the failure it gets reported with. An empty result means the order
	 * can't even be named - order-service will have nothing to match the
	 * failure against, which is the price of keeping the whole event in a
	 * single field.
	 */
	public Optional<OrderCreatedPayload> tryReadPayload(MapRecord<String, String, String> record) {
		try {
			return Optional.ofNullable(parse(record).payload());
		} catch (RuntimeException ex) {
			log.warn("Could not decode order event streamId={} while reporting failure: {}",
					record.getId(), ex.getMessage());
			return Optional.empty();
		}
	}

	private EventEnvelope<OrderCreatedPayload> parse(MapRecord<String, String, String> record) {
		String json = record.getValue().get(EventEnvelope.STREAM_FIELD);
		if (json == null) {
			throw new IllegalStateException("Stream entry " + record.getId() + " has no '"
					+ EventEnvelope.STREAM_FIELD + "' field: " + record.getValue());
		}

		return objectMapper.readValue(json, ORDER_EVENT);
	}
}
