-- Métricas do teste de carga, calculadas em notifications_db (vale para polling E CDC).
-- Latência ponta-a-ponta = processed_at − event_created_at (criação do pedido → processado pelo consumer).
-- span = janela do primeiro createdAt ao último processed_at (tempo total de drenagem da rajada).
-- throughput = mensagens processadas / span.
SELECT
  count(*)                                                                                    AS processed,
  round((percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM (processed_at - event_created_at)))) * 1000) AS p50_ms,
  round((percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM (processed_at - event_created_at)))) * 1000) AS p95_ms,
  round(max(extract(epoch FROM (processed_at - event_created_at))) * 1000)                    AS max_ms,
  round(extract(epoch FROM (max(processed_at) - min(event_created_at))) * 1000)               AS span_ms,
  round(count(*) / nullif(extract(epoch FROM (max(processed_at) - min(event_created_at))), 0)) AS throughput_msg_s
FROM processed_messages
WHERE event_created_at IS NOT NULL;
