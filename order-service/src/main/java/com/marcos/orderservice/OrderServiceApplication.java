package com.marcos.orderservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Aplicação produtora — lado do <b>Transactional Outbox</b>.
 *
 * <p>{@link EnableScheduling} habilita o {@code OutboxPublisher}, que faz o polling da
 * tabela outbox e publica os eventos no Kafka.
 */
@SpringBootApplication
@EnableScheduling
public class OrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
