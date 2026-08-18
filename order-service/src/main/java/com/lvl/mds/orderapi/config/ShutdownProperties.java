package com.lvl.mds.orderapi.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binds {@code order.shutdown.*} - how long shutdown is willing to wait for
 * work that is already in progress. The web side of the same question is
 * {@code server.shutdown=graceful} plus
 * {@code spring.lifecycle.timeout-per-shutdown-phase}, which Spring Boot
 * already exposes.
 *
 * @param listenerDrainTimeoutMs how long to wait for the result stream
 *                               listener to finish the result it is applying
 *                               before the context closes anyway. Keep it
 *                               below {@code spring.lifecycle.timeout-per-shutdown-phase},
 *                               and keep that below the container runtime's
 *                               grace period ({@code stop_grace_period} in
 *                               compose), or the process is killed mid-drain
 *                               and the wait buys nothing.
 */
@ConfigurationProperties(prefix = "order.shutdown")
public record ShutdownProperties(long listenerDrainTimeoutMs) {
}
