package com.marcos.orderservice.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * Linha da tabela {@code outbox}.
 *
 * <p>Cada instância representa um evento de domínio que <b>precisa</b> ser publicado no Kafka.
 * É gravada na mesma transação que altera o estado de negócio (o pedido), garantindo que
 * "mudou o estado" e "tem evento para publicar" são atômicos — a essência do padrão Outbox.
 *
 * <p>O {@code payload} é o JSON do evento; {@code messageId} é o id de idempotência propagado
 * ao consumer via header Kafka.
 */
@Entity
@Table(name = "outbox")
public class OutboxMessage {

    @Id
    @Column(name = "message_id")
    private UUID messageId;

    @Column(name = "aggregate_type", nullable = false)
    private String aggregateType;

    @Column(name = "aggregate_id", nullable = false)
    private String aggregateId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", nullable = false, columnDefinition = "jsonb")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private OutboxStatus status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    protected OutboxMessage() {
        // exigido pelo JPA
    }

    public OutboxMessage(String aggregateType, String aggregateId, String eventType, String payload) {
        this.messageId = UUID.randomUUID();
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
        this.status = OutboxStatus.PENDING;
        this.createdAt = Instant.now();
    }

    /** Marca como publicada após o envio bem-sucedido ao Kafka. */
    public void markSent() {
        this.status = OutboxStatus.SENT;
        this.publishedAt = Instant.now();
    }

    public UUID getMessageId() {
        return messageId;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getPayload() {
        return payload;
    }

    public OutboxStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }
}
