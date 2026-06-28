package com.marcos.orderservice.outbox;

import com.marcos.events.Headers;
import com.marcos.events.Topics;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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

        // Envia o lote inteiro "em voo" (sem join por mensagem) e só então espera todos os acks.
        // Isso paraleliza os envios — o producer idempotente (enable.idempotence) cuida de duplicatas
        // de retry — em vez de bloquear mensagem a mensagem. É o que destrava a vazão do publisher
        // sob carga (ver seção de teste de carga no README): o gargalo era o join() serial, não o lote.
        List<CompletableFuture<?>> acks = new ArrayList<>(batch.size());
        for (OutboxMessage message : batch) {
            acks.add(send(message));
        }

        // Barreira única: só seguimos quando TODAS as mensagens foram confirmadas (acks=all). Se
        // qualquer uma falhar, o join lança, a transação aborta e NENHUMA linha vira SENT (republica
        // no próximo ciclo). Mantém a garantia "marca SENT apenas após o broker confirmar".
        CompletableFuture.allOf(acks.toArray(new CompletableFuture[0])).join();

        for (OutboxMessage message : batch) {
            message.markSent();
        }
        log.info("Outbox: publicadas {} mensagem(ns) no tópico {}", batch.size(), Topics.ORDER_EVENTS);
    }

    private CompletableFuture<?> send(OutboxMessage message) {
        // Chave = aggregateId (id do pedido): garante ordenação por pedido dentro da partição.
        ProducerRecord<String, String> record =
                new ProducerRecord<>(Topics.ORDER_EVENTS, message.getAggregateId(), message.getPayload());
        record.headers().add(Headers.MESSAGE_ID, message.getMessageId().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add(Headers.EVENT_TYPE, message.getEventType().getBytes(StandardCharsets.UTF_8));
        return kafkaTemplate.send(record);
    }
}
