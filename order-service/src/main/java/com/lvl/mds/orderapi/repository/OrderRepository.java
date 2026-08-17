package com.lvl.mds.orderapi.repository;

import com.lvl.mds.orderapi.model.Order;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory DAO for orders - no database, per the task's scope. State
 * resets on restart, same as inventory-service's stock.
 */
@Repository
public class OrderRepository {

	private final Map<String, Order> orders = new ConcurrentHashMap<>();

	public Order save(Order order) {
		orders.put(order.getOrderId(), order);
		return order;
	}

	public Optional<Order> findById(String orderId) {
		return Optional.ofNullable(orders.get(orderId));
	}

	public List<Order> findAll() {
		return List.copyOf(orders.values());
	}

	public boolean existsById(String orderId) {
		return orders.containsKey(orderId);
	}

	public boolean deleteById(String orderId) {
		return orders.remove(orderId) != null;
	}
}
