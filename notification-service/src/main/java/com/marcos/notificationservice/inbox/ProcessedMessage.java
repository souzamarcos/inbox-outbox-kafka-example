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

    protected ProcessedMessage() {
        // exigido pelo JPA
    }

    public ProcessedMessage(UUID messageId) {
        this.messageId = messageId;
        this.processedAt = Instant.now();
    }

    public UUID getMessageId() {
        return messageId;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }
}
