package com.marcos.orderservice.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Agregado de pedido. É a entidade de negócio gravada na tabela {@code orders}.
 *
 * <p>O ponto central do padrão Outbox acontece em {@code OrderService}: esta entidade e a
 * linha correspondente na tabela {@code outbox} são persistidas na <b>mesma transação</b>.
 */
@Entity
@Table(name = "orders")
public class Order {

    @Id
    private UUID id;

    @Column(nullable = false)
    private String customer;

    @Column(nullable = false)
    private BigDecimal amount;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected Order() {
        // exigido pelo JPA
    }

    public Order(UUID id, String customer, BigDecimal amount, Instant createdAt) {
        this.id = id;
        this.customer = customer;
        this.amount = amount;
        this.createdAt = createdAt;
    }

    public static Order create(String customer, BigDecimal amount) {
        return new Order(UUID.randomUUID(), customer, amount, Instant.now());
    }

    public UUID getId() {
        return id;
    }

    public String getCustomer() {
        return customer;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
