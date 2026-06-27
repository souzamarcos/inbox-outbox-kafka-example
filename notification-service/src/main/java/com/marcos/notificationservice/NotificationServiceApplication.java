package com.marcos.notificationservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Aplicação consumidora — lado do <b>Inbox / Idempotent Consumer</b>.
 *
 * <p>Consome eventos de {@code orders.events}, deduplica pela tabela {@code processed_messages}
 * e, em caso de falha, faz retry com backoff e roteia para a DLT.
 */
@SpringBootApplication
public class NotificationServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(NotificationServiceApplication.class, args);
    }
}
