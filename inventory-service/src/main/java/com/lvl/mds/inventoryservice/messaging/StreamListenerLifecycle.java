package com.lvl.mds.inventoryservice.messaging;

import com.lvl.mds.inventoryservice.messaging.consumers.OrderEventConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;

import java.time.Duration;

/**
 * Owns the start and - the point of this class - the orderly stop of the
 * {@link StreamMessageListenerContainer} that feeds {@link OrderEventConsumer}.
 *
 * <p>{@code container.stop()} on its own is not a graceful stop: it flips the
 * subscription's state to cancelled and returns immediately, while the polling
 * thread may still be blocked in {@code XREADGROUP} or - the case that matters -
 * halfway through {@link OrderEventConsumer#onMessage}. The poll loop only
 * re-checks that flag after it has finished emitting the batch it already read,
 * so the in-flight order does get processed to completion; nothing waits for it
 * though. Since the Redis connection factory shuts down in a later phase
 * (its default is phase {@code 0}), letting the context close race ahead means
 * the {@code XACK} at the end of {@link OrderEventConsumer#onMessage} can fail
 * against a connection that is already gone. The order was reserved and its
 * result published, but the entry stays in the Pending Entries List and gets
 * redelivered after restart - correctness is preserved by the idempotency
 * store, at the cost of work that did not need repeating.
 *
 * <p>So {@link #stop()} cancels and then waits for {@link Subscription#isActive()}
 * to go false, which is exactly "the poll thread has left the event loop".
 * The wait is bounded: a consumer wedged on something slow must not hold the
 * shutdown open forever, and the timeout is deliberately shorter than
 * {@code spring.lifecycle.timeout-per-shutdown-phase} so this class, not the
 * lifecycle processor, is what gives up first and says so in the log.
 *
 * <p>Phase {@link Integer#MAX_VALUE} matches the container's own default and
 * means this stops before anything else in the context: no new orders are
 * taken while the rest of the application is still fully alive to finish the
 * current one.
 */
public class StreamListenerLifecycle implements SmartLifecycle {

	private static final Logger log = LoggerFactory.getLogger(StreamListenerLifecycle.class);

	/** How often {@link Subscription#isActive()} is sampled while draining. */
	private static final long DRAIN_POLL_INTERVAL_MS = 50;

	private final StreamMessageListenerContainer<String, MapRecord<String, String, String>> container;
	private final Consumer consumer;
	private final StreamOffset<String> streamOffset;
	private final StreamListener<String, MapRecord<String, String, String>> listener;
	private final Duration drainTimeout;

	private final Object lifecycleLock = new Object();

	private volatile boolean running;
	private volatile Subscription subscription;

	public StreamListenerLifecycle(
			StreamMessageListenerContainer<String, MapRecord<String, String, String>> container,
			Consumer consumer,
			StreamOffset<String> streamOffset,
			StreamListener<String, MapRecord<String, String, String>> listener,
			Duration drainTimeout) {
		this.container = container;
		this.consumer = consumer;
		this.streamOffset = streamOffset;
		this.listener = listener;
		this.drainTimeout = drainTimeout;
	}

	/**
	 * Registers the subscription and starts polling. Running here rather than
	 * in the {@code @Bean} method means consumption starts in the context's
	 * lifecycle phase - after every singleton has been constructed, so the
	 * consumer group exists and the inventory is seeded before the first
	 * {@code XREADGROUP}.
	 */
	@Override
	public void start() {
		synchronized (lifecycleLock) {
			if (running) {
				return;
			}

			subscription = container.receive(consumer, streamOffset, listener);
			container.start();
			running = true;

			log.info("Stream listener started: consumer '{}' of group '{}' on stream '{}'",
					consumer.getName(), consumer.getGroup(), streamOffset.getKey());
		}
	}

	@Override
	public void stop() {
		synchronized (lifecycleLock) {
			if (!running) {
				return;
			}
			running = false;

			log.info("Stopping stream listener - no further orders will be read");
			container.stop();
			awaitInFlightMessage();
		}
	}

	@Override
	public boolean isRunning() {
		return running;
	}

	@Override
	public int getPhase() {
		return Integer.MAX_VALUE;
	}

	/**
	 * Waits for the polling thread to leave its event loop, which it does only
	 * after the message it is currently handling has been processed and
	 * acknowledged.
	 */
	private void awaitInFlightMessage() {
		Subscription current = subscription;
		if (current == null) {
			return;
		}

		long deadline = System.nanoTime() + drainTimeout.toNanos();

		while (current.isActive() && System.nanoTime() < deadline) {
			try {
				Thread.sleep(DRAIN_POLL_INTERVAL_MS);
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
				break;
			}
		}

		if (current.isActive()) {
			log.warn("Stream listener still busy after {} - shutting down anyway. The in-flight order stays "
					+ "unacknowledged in the Pending Entries List and is redelivered on restart", drainTimeout);
		} else {
			log.info("Stream listener drained - no order left in flight");
		}
	}
}
