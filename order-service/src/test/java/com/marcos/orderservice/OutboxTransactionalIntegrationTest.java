package com.marcos.orderservice;

import static org.assertj.core.api.Assertions.assertThat;

import com.marcos.orderservice.application.OrderService;
import com.marcos.orderservice.domain.Order;
import com.marcos.orderservice.domain.OrderRepository;
import com.marcos.orderservice.outbox.OutboxMessage;
import com.marcos.orderservice.outbox.OutboxRepository;
import com.marcos.orderservice.outbox.OutboxStatus;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Verifica o invariante central do Outbox: criar um pedido grava, na MESMA transação, a entidade
 * de negócio e a mensagem de outbox correspondente (status PENDING).
 *
 * <p>O publisher é desligado ({@code outbox.publisher.enabled=false}) para que o teste observe a
 * linha PENDING antes de qualquer publicação. Postgres real via Testcontainers (precisa de Docker).
 */
@SpringBootTest(properties = {
        "outbox.publisher.enabled=false",
        "spring.kafka.bootstrap-servers=localhost:9092"
})
@Testcontainers
class OutboxTransactionalIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Autowired
    OrderService orderService;

    @Autowired
    OrderRepository orderRepository;

    @Autowired
    OutboxRepository outboxRepository;

    @Test
    void createOrder_persistsOrderAndOutboxMessageAtomically() {
        Order order = orderService.createOrder("Alice", new BigDecimal("100.00"));

        assertThat(orderRepository.findById(order.getId())).isPresent();

        List<OutboxMessage> outbox = outboxRepository.findAll();
        assertThat(outbox).hasSize(1);
        OutboxMessage message = outbox.getFirst();
        assertThat(message.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(message.getAggregateType()).isEqualTo("Order");
        assertThat(message.getAggregateId()).isEqualTo(order.getId().toString());
        assertThat(message.getEventType()).isEqualTo("OrderCreated");
        assertThat(message.getPayload()).contains(order.getId().toString());
    }
}
