-- Cria um database por serviço (mantém a semântica de "database por microsserviço"
-- usando uma única instância Postgres, para reduzir a infra do exemplo).
CREATE DATABASE orders_db OWNER app;
CREATE DATABASE notifications_db OWNER app;

-- Necessário para a alternativa CDC: o Debezium lê o WAL via slot de replicação lógica.
-- Inofensivo no modo polling.
ALTER ROLE app WITH REPLICATION;
