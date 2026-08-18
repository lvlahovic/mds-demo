package com.lvl.mds.orderapi.messaging.consumers;

import com.lvl.mds.orderapi.messaging.event.EventEnvelope;
import com.lvl.mds.orderapi.messaging.event.ReservationResultPayload;
import com.lvl.mds.orderapi.model.OrderStatus;
import com.lvl.mds.orderapi.services.OrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

/**
 * Translates a reservation-result event into a status transition on the
 * local order. Shared by the live consumer ({@link ReservationResultConsumer})
 * and the startup pending-entry drain in {@link ResultStreamInitializer}, so
 * both take the same path; neither acknowledges here - each caller owns its
 * own ack lifecycle.
 *
 * <p>This class owns the mapping between inventory-service's wire vocabulary
 * and this service's own {@link OrderStatus}. Keeping the translation in the
 * messaging layer is what lets the two services evolve their internal
 * enums independently - the only thing that has to stay agreed is the
 * {@link EventEnvelope} contract and the set of outcome strings inside it.
 */
@Component
public class ReservationResultProcessor {

	private static final TypeReference<EventEnvelope<ReservationResultPayload>> RESULT_EVENT =
			new TypeReference<>() {
			};

	private static final Logger log = LoggerFactory.getLogger(ReservationResultProcessor.class);

	private final OrderService orderService;
	private final ObjectMapper objectMapper;

	public ReservationResultProcessor(OrderService orderService, ObjectMapper objectMapper) {
		this.orderService = orderService;
		this.objectMapper = objectMapper;
	}

	public void process(MapRecord<String, String, String> record) {
		String json = record.getValue().get(EventEnvelope.STREAM_FIELD);
		if (json == null) {
			log.warn("Stream entry without an '{}' field - discarding (streamId={}, fields={})",
					EventEnvelope.STREAM_FIELD, record.getId(), record.getValue());
			return;
		}

		EventEnvelope<ReservationResultPayload> event = objectMapper.readValue(json, RESULT_EVENT);
		event.requireSupportedContract(ReservationResultPayload.EVENT_TYPE);
		ReservationResultPayload payload = event.payload();

		if (payload == null || payload.orderId() == null || payload.orderId().isBlank()) {
			log.warn("Reservation result without an orderId - discarding (eventId={}, streamId={})",
					event.eventId(), record.getId());
			return;
		}

		orderService.applyReservationResult(payload.orderId(), toStatus(payload.outcome()), reason(payload));
	}

	private OrderStatus toStatus(String outcome) {
		return switch (outcome == null ? "" : outcome) {
			case "RESERVED" -> OrderStatus.RESERVED;
			case "INSUFFICIENT_STOCK" -> OrderStatus.REJECTED_INSUFFICIENT_STOCK;
			case "ITEM_NOT_FOUND" -> OrderStatus.REJECTED_UNKNOWN_ITEM;
			case "FAILED" -> OrderStatus.FAILED;
			// An outcome this version doesn't know about is still an answer,
			// and it is not a reservation - treating it as FAILED is the
			// honest reading and keeps the order from hanging in PUBLISHED.
			// Note this is a value the contract allows to grow, unlike an
			// unknown schemaVersion, which is rejected outright.
			default -> {
				log.warn("Unrecognized reservation outcome '{}' - treating the order as FAILED", outcome);
				yield OrderStatus.FAILED;
			}
		};
	}

	private String reason(ReservationResultPayload payload) {
		String reason = payload.reason();
		return reason == null || reason.isBlank() ? "reported as '" + payload.outcome() + "'" : reason;
	}
}
