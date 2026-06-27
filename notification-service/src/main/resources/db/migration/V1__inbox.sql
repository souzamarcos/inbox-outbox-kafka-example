-- Tabela Inbox: registra os ids de mensagens já processadas para garantir idempotência.
-- A PK em message_id é a barreira definitiva contra processamento duplicado.
CREATE TABLE processed_messages (
    message_id   UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL
);
