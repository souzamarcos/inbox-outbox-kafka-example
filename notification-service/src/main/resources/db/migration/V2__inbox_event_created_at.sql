-- Instrumentação para medição de latência ponta-a-ponta (teste de carga Scheduled × CDC).
-- Guarda o 'createdAt' do evento (momento em que o pedido foi criado no order-service) para
-- que a latência created_at → processed_at seja calculável direto em SQL, igual nos dois modos
-- de publicação (polling e CDC). Nullable: o caminho de deduplicação pura não preenche.
ALTER TABLE processed_messages ADD COLUMN event_created_at TIMESTAMPTZ;
