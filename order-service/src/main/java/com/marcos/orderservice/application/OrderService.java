package com.marcos.orderservice.application;

import com.marcos.events.OrderCreatedEvent;
import com.marcos.orderservice.domain.Order;
import com.marcos.orderservice.domain.OrderRepository;
import com.marcos.orderservice.outbox.OutboxMessage;
import com.marcos.orderservice.outbox.OutboxRepository;
import java.math.BigDecimal;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/**
 * Serviço de aplicação que cria pedidos — coração do padrão <b>Transactional Outbox</b>.
 *
 * <p>{@link #createOrder} grava o {@link Order} <b>e</b> a {@link OutboxMessage} dentro da
 * mesma transação ({@link Transactional}). Ou as duas linhas são commitadas, ou nenhuma.
 *
 * <p>Repare que aqui <b>não</b> publicamos no Kafka. Publicar dentro da transação reintroduziria
 * o problema do dual-write: o commit do Kafka não é atômico com o commit do banco, então uma
 * falha após publicar (mas antes de commitar) deixaria o sistema inconsistente. A publicação é
 * responsabilidade assíncrona do {@code OutboxPublisher}.
 */
@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public OrderService(OrderRepository orderRepository,
                        OutboxRepository outboxRepository,
                        ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public Order createOrder(String customer, BigDecimal amount) {
        Order order = Order.create(customer, amount);
        orderRepository.save(order);

        OrderCreatedEvent event = new OrderCreatedEvent(
                order.getId(), order.getCustomer(), order.getAmount(), order.getCreatedAt());

        OutboxMessage message = new OutboxMessage(
                "Order",
                order.getId().toString(),
                OrderCreatedEvent.TYPE,
                serialize(event));
        outboxRepository.save(message);

        return order;
    }

    private String serialize(OrderCreatedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JacksonException e) {
            // No Jackson 3 a exceção é unchecked; capturamos para abortar a transação com
            // uma mensagem clara — o pedido não é criado se o evento não serializa.
            throw new IllegalStateException("Falha ao serializar OrderCreatedEvent", e);
        }
    }
}
