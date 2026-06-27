package com.marcos.orderservice.outbox;

/** Estado de uma mensagem na tabela outbox. */
public enum OutboxStatus {
    /** Gravada na transação de negócio, ainda não publicada no Kafka. */
    PENDING,
    /** Já publicada no Kafka pelo publisher. Mantida para auditoria. */
    SENT
}
