package com.lvl.mds.orderapi;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Boots the whole application context with the result-stream listener turned
 * off - the API itself needs no live Redis to start, and the consumer would
 * otherwise try to create its consumer group against a broker that isn't
 * running in a unit build.
 */
@SpringBootTest(properties = "order.redis.result-listener-enabled=false")
class OrderApiApplicationTests {

	@Test
	void contextLoads() {
	}

}
