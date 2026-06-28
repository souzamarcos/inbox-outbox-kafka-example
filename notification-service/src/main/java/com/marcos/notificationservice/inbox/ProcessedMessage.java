package com.marcos.notificationservice.inbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/**
 * Registro de uma mensagem já processada — a tabela <b>Inbox</b>.
 *
 * <p>A chave primária é o {@code messageId} (o mesmo id gerado no outbox do producer). Antes de
 * processar um evento, o consumer tenta inserir aqui; se o id já existe, a mensagem é uma
 * duplicata e é ignorada. Isso transforma a entrega at-least-once do Kafka em
 * "efeito exactly-once" do ponto de vista de negócio.
 */
@Entity
@Table(name = "processed_messages")
public class ProcessedMessage {

    @Id
    @Column(name = "message_id")
    private UUID messageId;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    // Momento em que o pedido foi criado no order-service (vem do payload do evento). Usado só
    // para medir a latência ponta-a-ponta (processed_at − event_created_at) no teste de carga.
    @Column(name = "event_created_at")
    private Instant eventCreatedAt;

    protected ProcessedMessage() {
        // exigido pelo JPA
    }

    public ProcessedMessage(UUID messageId, Instant eventCreatedAt) {
        this.messageId = messageId;
        this.processedAt = Instant.now();
        this.eventCreatedAt = eventCreatedAt;
    }

    public UUID getMessageId() {
        return messageId;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }

    public Instant getEventCreatedAt() {
        return eventCreatedAt;
    }
}
