-- Tabela de negócio: pedidos.
CREATE TABLE orders (
    id         UUID PRIMARY KEY,
    customer   VARCHAR(255)   NOT NULL,
    amount     NUMERIC(19, 2) NOT NULL,
    created_at TIMESTAMPTZ    NOT NULL
);

-- Tabela Outbox: eventos a publicar, gravados na MESMA transação que altera 'orders'.
CREATE TABLE outbox (
    message_id     UUID PRIMARY KEY,
    aggregate_type VARCHAR(255) NOT NULL,
    aggregate_id   VARCHAR(255) NOT NULL,
    event_type     VARCHAR(255) NOT NULL,
    payload        JSONB        NOT NULL,
    status         VARCHAR(20)  NOT NULL,
    created_at     TIMESTAMPTZ  NOT NULL,
    published_at   TIMESTAMPTZ
);

-- Índice para o polling do publisher: busca eficiente das pendentes em ordem de criação.
CREATE INDEX idx_outbox_pending ON outbox (status, created_at);
