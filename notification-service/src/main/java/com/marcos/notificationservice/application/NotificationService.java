package com.marcos.notificationservice.application;

import com.marcos.events.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Efeito de negócio do consumer: "enviar uma notificação" sobre o pedido criado.
 *
 * <p>Aqui é só um log (é um exemplo), mas representa o ponto onde um efeito colateral real
 * aconteceria. É exatamente esse efeito que o Inbox protege de execução duplicada.
 *
 * <p>Gatilho didático para demonstrar retry + DLT: se o nome do cliente contiver "fail",
 * o processamento lança exceção, forçando os retries e o roteamento para a Dead Letter Topic.
 */
@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    public void handle(OrderCreatedEvent event) {
        if (event.customer() != null && event.customer().toLowerCase().contains("fail")) {
            throw new IllegalStateException(
                    "Falha simulada ao processar pedido " + event.orderId() + " (cliente '" + event.customer() + "')");
        }
        log.info("Notificação enviada: pedido {} do cliente {} no valor {}",
                event.orderId(), event.customer(), event.amount());
    }
}
