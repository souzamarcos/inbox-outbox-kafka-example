package com.marcos.events;

/**
 * Nomes de headers Kafka usados no exemplo.
 *
 * <p>O {@link #MESSAGE_ID} é a peça central da deduplicação do Inbox: ele identifica
 * unicamente o evento (não a mensagem física no Kafka) e é o mesmo valor gravado na
 * coluna {@code message_id} da tabela outbox. O consumer usa esse id como chave de
 * idempotência na tabela {@code processed_messages}.
 */
public final class Headers {

    /** Identificador único do evento (UUID), propagado do outbox até o inbox. */
    public static final String MESSAGE_ID = "messageId";

    /** Tipo do evento (ex.: "OrderCreated"), útil para roteamento/observabilidade. */
    public static final String EVENT_TYPE = "eventType";

    private Headers() {
    }
}
