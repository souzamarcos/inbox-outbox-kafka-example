package com.marcos.events;

/**
 * Nomes de tópicos Kafka usados no exemplo.
 *
 * <p>Mantidos em um único lugar para que producer e consumer concordem sobre o contrato
 * de transporte.
 */
public final class Topics {

    /** Tópico onde os eventos de pedido são publicados. */
    public static final String ORDER_EVENTS = "orders.events";

    /** Dead Letter Topic: mensagens que falharam o processamento após os retries. */
    public static final String ORDER_EVENTS_DLT = "orders.events.DLT";

    private Topics() {
    }
}
