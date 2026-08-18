package com.lvl.mds.orderapi.messaging;

import com.lvl.mds.orderapi.model.OrderStatus;
import com.lvl.mds.orderapi.services.OrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.stereotype.Component;

import java.util.Map;

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
 * enums independently - the only thing that has to stay agreed is the set of
 * strings on the stream.
 */
@Component
public class ReservationResultProcessor {

	private static final Logger log = LoggerFactory.getLogger(ReservationResultProcessor.class);

	private final OrderService orderService;

	public ReservationResultProcessor(OrderService orderService) {
		this.orderService = orderService;
	}

	public void process(MapRecord<String, String, String> record) {
		Map<String, String> fields = record.getValue();
		String orderId = fields.get("orderId");
		String outcome = fields.get("outcome");

		if (orderId == null || orderId.isBlank()) {
			log.warn("Reservation result without an orderId - discarding (streamId={}, fields={})",
					record.getId(), fields);
			return;
		}

		orderService.applyReservationResult(orderId, toStatus(outcome), reason(fields, outcome));
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
			default -> {
				log.warn("Unrecognized reservation outcome '{}' - treating the order as FAILED", outcome);
				yield OrderStatus.FAILED;
			}
		};
	}

	private String reason(Map<String, String> fields, String outcome) {
		String reason = fields.get("reason");
		return reason == null || reason.isBlank() ? "reported as '" + outcome + "'" : reason;
	}
}
