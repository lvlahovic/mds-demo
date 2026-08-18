package com.lvl.mds.inventoryservice.messaging.stream;

import com.lvl.mds.inventoryservice.messaging.StreamListenerLifecycle;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Covers why {@link StreamListenerLifecycle#stop()} is more than a call to
 * {@code container.stop()}: it must wait for the poll thread to actually
 * leave its event loop (an in-flight {@code onMessage} finishing its
 * {@code XACK}), and it must give up rather than block forever if that takes
 * too long.
 */
class StreamListenerLifecycleTest {

	private static final Consumer CONSUMER = Consumer.from("inventory-service-group", "consumer-1");
	private static final StreamOffset<String> OFFSET = StreamOffset.create("orders-stream", ReadOffset.lastConsumed());

	@Test
	@SuppressWarnings("unchecked")
	void startRegistersAndStartsTheContainer() {
		StreamMessageListenerContainer<String, MapRecord<String, String, String>> container = mock(StreamMessageListenerContainer.class);
		Subscription subscription = mock(Subscription.class);
		StreamListener<String, MapRecord<String, String, String>> listener = mock(StreamListener.class);
		doReturn(subscription).when(container).receive(any(Consumer.class), any(), any());

		StreamListenerLifecycle lifecycle =
				new StreamListenerLifecycle(container, CONSUMER, OFFSET, listener, Duration.ofMillis(500));

		lifecycle.start();

		verify(container).receive(CONSUMER, OFFSET, listener);
		verify(container).start();
		assertThat(lifecycle.isRunning()).isTrue();
	}

	@Test
	@SuppressWarnings("unchecked")
	void stopCancelsAndWaitsUntilTheSubscriptionIsNoLongerActive() {
		StreamMessageListenerContainer<String, MapRecord<String, String, String>> container = mock(StreamMessageListenerContainer.class);
		Subscription subscription = mock(Subscription.class);
		StreamListener<String, MapRecord<String, String, String>> listener = mock(StreamListener.class);
		doReturn(subscription).when(container).receive(any(Consumer.class), any(), any());

		// Simulates onMessage() still running while stop() polls isActive(),
		// then the poll thread finally exiting its event loop.
		AtomicBoolean stillHandlingInFlightMessage = new AtomicBoolean(true);
		doAnswer(inv -> stillHandlingInFlightMessage.get()).when(subscription).isActive();

		StreamListenerLifecycle lifecycle =
				new StreamListenerLifecycle(container, CONSUMER, OFFSET, listener, Duration.ofSeconds(5));
		lifecycle.start();

		Thread stopper = new Thread(lifecycle::stop);
		stopper.start();

		// Give stop() a moment to be blocked inside the drain wait, then let
		// the "in-flight message" finish.
		await(() -> subscriptionPolledAtLeastTwice(subscription));
		stillHandlingInFlightMessage.set(false);

		joinQuietly(stopper);

		verify(container).stop();
		assertThat(lifecycle.isRunning()).isFalse();
	}

	@Test
	@SuppressWarnings("unchecked")
	void stopGivesUpAfterTheDrainTimeoutInsteadOfBlockingForever() {
		StreamMessageListenerContainer<String, MapRecord<String, String, String>> container = mock(StreamMessageListenerContainer.class);
		Subscription subscription = mock(Subscription.class);
		StreamListener<String, MapRecord<String, String, String>> listener = mock(StreamListener.class);
		doReturn(subscription).when(container).receive(any(Consumer.class), any(), any());
		doReturn(true).when(subscription).isActive(); // never finishes on its own

		StreamListenerLifecycle lifecycle =
				new StreamListenerLifecycle(container, CONSUMER, OFFSET, listener, Duration.ofMillis(200));
		lifecycle.start();

		long start = System.nanoTime();
		lifecycle.stop();
		long elapsedMs = Duration.ofNanos(System.nanoTime() - start).toMillis();

		verify(container).stop();
		assertThat(elapsedMs).isLessThan(2_000);
	}

	@Test
	@SuppressWarnings("unchecked")
	void stopIsANoOpWhenNeverStarted() {
		StreamMessageListenerContainer<String, MapRecord<String, String, String>> container = mock(StreamMessageListenerContainer.class);
		StreamListener<String, MapRecord<String, String, String>> listener = mock(StreamListener.class);

		StreamListenerLifecycle lifecycle =
				new StreamListenerLifecycle(container, CONSUMER, OFFSET, listener, Duration.ofMillis(500));

		lifecycle.stop();

		verify(container, never()).stop();
	}

	private static boolean subscriptionPolledAtLeastTwice(Subscription subscription) {
		try {
			verify(subscription, atLeast(2)).isActive();
			return true;
		} catch (AssertionError notYet) {
			return false;
		}
	}

	private static void await(java.util.function.BooleanSupplier condition) {
		long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
		while (!condition.getAsBoolean()) {
			if (System.nanoTime() > deadline) {
				throw new AssertionError("Condition not met within timeout");
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
				throw new RuntimeException(ex);
			}
		}
	}

	private static void joinQuietly(Thread thread) {
		try {
			thread.join(Duration.ofSeconds(5).toMillis());
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
	}
}
