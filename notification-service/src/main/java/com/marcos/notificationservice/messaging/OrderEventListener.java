package com.marcos.notificationservice.messaging;

import com.marcos.events.Headers;
import com.marcos.events.OrderCreatedEvent;
import com.marcos.events.Topics;
import com.marcos.notificationservice.inbox.InboxService;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * Listener Kafka do tópico {@link Topics#ORDER_EVENTS}.
 *
 * <p>Responsabilidades: extrair o {@code messageId} do header, desserializar o payload e delegar
 * ao {@link InboxService}, que cuida da deduplicação idempotente. Exceções lançadas aqui são
 * tratadas pelo error handler (retry + DLT) configurado em {@code KafkaConsumerConfig}.
 */
@Component
public class OrderEventListener {

    private final InboxService inboxService;
    private final ObjectMapper objectMapper;

    public OrderEventListener(InboxService inboxService, ObjectMapper objectMapper) {
        this.inboxService = inboxService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = Topics.ORDER_EVENTS, groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(ConsumerRecord<String, String> record) {
        UUID messageId = extractMessageId(record);
        OrderCreatedEvent event = objectMapper.readValue(record.value(), OrderCreatedEvent.class);
        inboxService.process(messageId, event);
    }

    private UUID extractMessageId(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(Headers.MESSAGE_ID);
        if (header == null) {
            throw new IllegalStateException(
                    "Mensagem sem header '" + Headers.MESSAGE_ID + "' — não é possível deduplicar");
        }
        return UUID.fromString(new String(header.value(), StandardCharsets.UTF_8));
    }
}
