package com.marcos.orderservice.outbox;

import com.marcos.events.Headers;
import com.marcos.events.Topics;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Publisher do Outbox via <b>polling</b> (mecanismo principal deste exemplo).
 *
 * <p>A cada intervalo, lê um lote de mensagens {@code PENDING}, publica cada uma no Kafka e
 * marca como {@code SENT}. A leitura usa {@code FOR UPDATE SKIP LOCKED} (ver
 * {@link OutboxRepository#findPendingBatch}), então múltiplas instâncias podem rodar em paralelo.
 *
 * <p>Garantia de entrega: <b>at-least-once</b>. Se o processo cair após publicar no Kafka mas
 * antes de commitar o {@code SENT}, a mensagem será republicada no próximo ciclo. Por isso o
 * consumer precisa do Inbox (deduplicação idempotente).
 *
 * <p>Desabilitável via {@code outbox.publisher.enabled=false} — útil ao demonstrar a alternativa
 * CDC (Debezium), em que a publicação acontece fora da aplicação.
 */
@Component
@ConditionalOnProperty(name = "outbox.publisher.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${outbox.publisher.batch-size:100}")
    private int batchSize;

    public OutboxPublisher(OutboxRepository outboxRepository, KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelayString = "${outbox.publisher.poll-delay-ms:1000}")
    @Transactional
    public void publishPending() {
        List<OutboxMessage> batch = outboxRepository.findPendingBatch(batchSize);
        if (batch.isEmpty()) {
            return;
        }

        for (OutboxMessage message : batch) {
            publish(message);
            message.markSent();
        }
        log.info("Outbox: publicadas {} mensagem(ns) no tópico {}", batch.size(), Topics.ORDER_EVENTS);
    }

    private void publish(OutboxMessage message) {
        // Chave = aggregateId (id do pedido): garante ordenação por pedido dentro da partição.
        ProducerRecord<String, String> record =
                new ProducerRecord<>(Topics.ORDER_EVENTS, message.getAggregateId(), message.getPayload());
        record.headers().add(Headers.MESSAGE_ID, message.getMessageId().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add(Headers.EVENT_TYPE, message.getEventType().getBytes(StandardCharsets.UTF_8));

        // .join() torna a publicação síncrona dentro do laço: só marcamos SENT após o broker
        // confirmar (acks=all). Se falhar, a exceção aborta a transação e nada vira SENT.
        kafkaTemplate.send(record).join();
    }
}
