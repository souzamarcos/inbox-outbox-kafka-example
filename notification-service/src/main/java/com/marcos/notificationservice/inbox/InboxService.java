package com.marcos.notificationservice.inbox;

import com.marcos.events.OrderCreatedEvent;
import com.marcos.notificationservice.application.NotificationService;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Orquestra o consumo idempotente — o coração do padrão <b>Inbox</b>.
 *
 * <p>{@link #process} roda em uma única transação que abrange tanto a deduplicação quanto o
 * efeito de negócio:
 * <ol>
 *   <li>Se {@code messageId} já está em {@code processed_messages} → duplicata, ignora.</li>
 *   <li>Senão, executa o efeito de negócio <b>e</b> grava o {@code messageId}.</li>
 * </ol>
 *
 * <p>Como as duas coisas estão na mesma transação: se o negócio falhar, nada é commitado e a
 * mensagem volta para retry; se tudo der certo, o {@code messageId} fica gravado e uma reentrega
 * futura (at-least-once) será reconhecida como duplicata. A PK da tabela é a garantia final
 * contra corrida.
 */
@Service
public class InboxService {

    private static final Logger log = LoggerFactory.getLogger(InboxService.class);

    private final ProcessedMessageRepository processedMessages;
    private final NotificationService notificationService;

    public InboxService(ProcessedMessageRepository processedMessages, NotificationService notificationService) {
        this.processedMessages = processedMessages;
        this.notificationService = notificationService;
    }

    @Transactional
    public void process(UUID messageId, OrderCreatedEvent event) {
        if (processedMessages.existsById(messageId)) {
            log.info("Inbox: mensagem {} já processada — ignorando duplicata", messageId);
            return;
        }

        // Efeito de negócio primeiro: se lançar, a transação é desfeita e a mensagem é reprocessada.
        notificationService.handle(event);

        // Marca como processada na MESMA transação do efeito de negócio.
        processedMessages.save(new ProcessedMessage(messageId));
    }
}
