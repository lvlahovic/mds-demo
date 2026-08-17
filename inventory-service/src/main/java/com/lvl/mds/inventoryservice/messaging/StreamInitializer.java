package com.lvl.mds.inventoryservice.messaging;

import com.lvl.mds.inventoryservice.config.RedisStreamProperties;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Creates the {@code inventory-service-group} consumer group on startup so
 * the service can come up before order-service has ever published anything
 * (Spring Data Redis issues {@code XGROUP CREATE ... MKSTREAM}, which also
 * creates the stream if missing). Safe to run on every startup - an
 * already-existing group just raises BUSYGROUP, which is caught and ignored.
 */
@Component
public class StreamInitializer {

	private static final Logger log = LoggerFactory.getLogger(StreamInitializer.class);

	private final StreamOperations<String, String, String> streamOps;
	private final RedisStreamProperties properties;

	public StreamInitializer(StringRedisTemplate redisTemplate, RedisStreamProperties properties) {
		this.streamOps = redisTemplate.opsForStream();
		this.properties = properties;
	}

	@PostConstruct
	public void createConsumerGroup() {
		try {
			streamOps.createGroup(properties.streamKey(), ReadOffset.from("0"), properties.consumerGroup());
			log.info("Created consumer group '{}' on stream '{}'", properties.consumerGroup(), properties.streamKey());
		} catch (DataAccessException ex) {
			if (isBusyGroup(ex)) {
				log.info("Consumer group '{}' already exists on stream '{}'", properties.consumerGroup(), properties.streamKey());
			} else {
				throw ex;
			}
		}
	}

	/**
	 * Spring Data Redis wraps the driver exception (e.g.
	 * {@code RedisSystemException}), whose own message is a generic
	 * "Error in execution" - the actual {@code BUSYGROUP} text from Redis is
	 * on the cause. Walk the chain instead of checking {@code ex.getMessage()}
	 * alone.
	 */
	private boolean isBusyGroup(Throwable ex) {
		for (Throwable current = ex; current != null; current = current.getCause()) {
			if (current.getMessage() != null && current.getMessage().contains("BUSYGROUP")) {
				return true;
			}
		}
		return false;
	}
}
