package com.marcos.notificationservice;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.marcos.events.OrderCreatedEvent;
import com.marcos.notificationservice.application.NotificationService;
import com.marcos.notificationservice.inbox.InboxService;
import com.marcos.notificationservice.inbox.ProcessedMessageRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Verifica a idempotência do Inbox: processar a MESMA mensagem (mesmo messageId) duas vezes
 * executa o efeito de negócio apenas uma vez e grava um único registro em processed_messages.
 *
 * <p>O listener Kafka é desligado ({@code spring.kafka.listener.auto-startup=false}); chamamos o
 * {@link InboxService} diretamente. Postgres real via Testcontainers (precisa de Docker).
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=localhost:9092",
        "spring.kafka.listener.auto-startup=false"
})
@Testcontainers
class InboxDeduplicationIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Autowired
    InboxService inboxService;

    @Autowired
    ProcessedMessageRepository processedMessages;

    @MockitoSpyBean
    NotificationService notificationService;

    @Test
    void processingSameMessageTwice_runsBusinessEffectOnce() {
        UUID messageId = UUID.randomUUID();
        OrderCreatedEvent event = new OrderCreatedEvent(
                UUID.randomUUID(), "Bob", new BigDecimal("42.00"), Instant.now());

        inboxService.process(messageId, event);
        inboxService.process(messageId, event); // duplicata: deve ser ignorada

        verify(notificationService, times(1)).handle(event);
        assertThat(processedMessages.count()).isEqualTo(1);
        assertThat(processedMessages.existsById(messageId)).isTrue();
    }
}
