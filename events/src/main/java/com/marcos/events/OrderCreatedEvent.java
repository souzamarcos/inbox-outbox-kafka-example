package com.marcos.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Evento de domínio publicado quando um pedido é criado.
 *
 * <p>É o "payload" serializado como JSON no corpo da mensagem Kafka. Os metadados de
 * entrega (messageId, eventType) viajam em {@link Headers headers}, não aqui.
 *
 * @param orderId   id do pedido (também usado como chave Kafka, para garantir ordem por pedido)
 * @param customer  nome do cliente
 * @param amount    valor total do pedido
 * @param createdAt momento da criação
 */
public record OrderCreatedEvent(
        UUID orderId,
        String customer,
        BigDecimal amount,
        Instant createdAt) {

    /** Nome estável do tipo de evento, usado no header {@link Headers#EVENT_TYPE}. */
    public static final String TYPE = "OrderCreated";
}
