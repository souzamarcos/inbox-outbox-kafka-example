package com.marcos.orderservice.outbox;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OutboxRepository extends JpaRepository<OutboxMessage, UUID> {

    /**
     * Busca um lote de mensagens pendentes para publicação.
     *
     * <p>Query nativa com <b>{@code FOR UPDATE SKIP LOCKED}</b>: cada instância do publisher trava
     * apenas as linhas que vai processar e <b>pula</b> as já travadas por outra instância. Assim é
     * possível rodar vários publishers em paralelo sem publicar a mesma mensagem duas vezes nem
     * ficar bloqueado esperando lock. {@code ORDER BY created_at} preserva a ordem de criação.
     */
    @Query(value = """
            SELECT * FROM outbox
            WHERE status = 'PENDING'
            ORDER BY created_at ASC
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxMessage> findPendingBatch(@Param("limit") int limit);
}
