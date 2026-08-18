package com.lvl.mds.orderapi.messaging.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.UUID;

/**
 * Wrapper every event on a Redis Stream is published inside: metadata about
 * the message itself, plus the business payload as a separate object. The
 * consumer can decide <em>what</em> arrived and <em>whether it can read it</em>
 * before it looks at the payload - without that, any change to the payload
 * shape is a breaking change nobody can detect.
 *
 * <p>The whole envelope goes onto the stream as one JSON string under a
 * single field ({@link #STREAM_FIELD}) rather than being spread over stream
 * fields. One format per message keeps the reading and writing sides short,
 * and versioning the payload never changes the shape of the stream entry.
 * The trade-off: the metadata can't be read without parsing the JSON, and a
 * corrupted blob loses even the id - acceptable here, where the only producer
 * is a typed record in a sibling service.
 *
 * <p>Deliberately absent: {@code correlationId} and {@code producer}. They
 * belong with request-scoped logging (MDC), which this project doesn't have
 * yet - a field nobody reads is harder to justify than a missing one.
 *
 * <p>This type exists as an identical copy in both services on purpose: they
 * are independent Maven builds with no shared module, so the contract is
 * agreed on the wire, not in Java. What catches drift between the copies is
 * a literal-JSON fixture on the reading side, not the compiler.
 *
 * @param <T> the payload type, e.g. {@link OrderCreatedPayload}
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record EventEnvelope<T>(

		/** Identifies this publication. Not the business key: deduplication is by orderId, this is the log trail. */
		String eventId,

		/** What happened, e.g. {@code order.created}. Lets one stream carry more than one kind of event. */
		String eventType,

		/** Major version of the payload contract, see {@link #SCHEMA_VERSION}. */
		int schemaVersion,

		/** When the producer created the event - not when the broker or the consumer saw it. */
		Instant occurredAt,

		T payload
) {

	/** Stream field the serialized envelope is stored under. */
	public static final String STREAM_FIELD = "event";

	/**
	 * The contract version this build writes, and the only one it reads.
	 *
	 * <p>Kept in code rather than in {@code application.properties} on
	 * purpose: the schema version is a property of the code that produces the
	 * payload, not of a deployment. In configuration it could be changed
	 * without the payload changing, which would just lie to the consumer.
	 *
	 * <p>Only the major number. Adding an optional field keeps the version as
	 * it is (existing consumers ignore what they don't know); removing,
	 * renaming or repurposing one bumps it. A minor number would suggest the
	 * consumer should look at it, and by definition it doesn't.
	 */
	public static final int SCHEMA_VERSION = 1;

	public static <T> EventEnvelope<T> of(String eventType, T payload) {
		return new EventEnvelope<>(UUID.randomUUID().toString(), eventType, SCHEMA_VERSION, Instant.now(), payload);
	}

	/**
	 * Rejects an event this build has no business interpreting. When a v2
	 * arrives, this check becomes "is the version in my supported set" and
	 * the consumer learns to read both before the producer switches over.
	 */
	public void requireSupportedContract(String expectedType) {
		if (!expectedType.equals(eventType)) {
			throw new IllegalStateException("Unexpected eventType '" + eventType + "', expected '" + expectedType + "'");
		}
		if (schemaVersion != SCHEMA_VERSION) {
			throw new IllegalStateException("Unsupported schemaVersion " + schemaVersion + " for '" + eventType
					+ "', this build reads version " + SCHEMA_VERSION);
		}
	}
}
