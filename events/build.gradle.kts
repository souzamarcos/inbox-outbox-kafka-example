// Módulo "events": contrato compartilhado entre producer e consumer.
// É uma biblioteca Java pura (sem Spring Boot) — só carrega os tipos do evento
// e as constantes (nomes de tópico e headers) que ambos os serviços precisam concordar.
//
// Trade-off didático: compartilhar classes Java acopla os serviços ao contrato em tempo de
// compilação. Em produção, geralmente se prefere um schema versionado (Avro/JSON Schema) +
// Schema Registry. Aqui mantemos simples de propósito.

plugins {
    `java-library`
}
