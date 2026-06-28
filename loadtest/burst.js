// Teste de carga — cenário BURST (rajada) para comparar Outbox via polling (@Scheduled) × CDC (Debezium).
//
// Dispara TOTAL requisições POST /orders o mais rápido possível (VUS workers em paralelo) e para.
// Isso enche a tabela `outbox` de uma vez; o mecanismo de publicação (polling ou CDC) drena depois.
// A diferença entre os modos aparece na latência ponta-a-ponta e no tempo de drenagem da rajada,
// medidos via SQL em `processed_messages` (ver README, seção de teste de carga).
//
// Rodar com a imagem oficial do k6 (não precisa instalar no host):
//   docker run --rm -i --network host -e TOTAL=5000 -e VUS=50 \
//     -v "$PWD/loadtest:/scripts" grafana/k6 run /scripts/burst.js
// No Docker Desktop (Windows/Mac) troque o alvo para host.docker.internal:
//   ... -e TARGET=http://host.docker.internal:8081 ...

import http from 'k6/http';
import { check } from 'k6';

const TARGET = __ENV.TARGET || 'http://host.docker.internal:8081';
const TOTAL = parseInt(__ENV.TOTAL || '5000', 10);
const VUS = parseInt(__ENV.VUS || '50', 10);

export const options = {
  scenarios: {
    burst: {
      executor: 'shared-iterations',
      vus: VUS,
      iterations: TOTAL,
      maxDuration: '3m',
    },
  },
  // Só nos interessa a fase de injeção; thresholds ficam informativos.
  thresholds: {
    http_req_failed: ['rate<0.01'],
  },
};

const payload = JSON.stringify({ customer: 'load', amount: 9.99 });
const params = { headers: { 'Content-Type': 'application/json' } };

export default function () {
  const res = http.post(`${TARGET}/orders`, payload, params);
  check(res, { 'status is 201': (r) => r.status === 201 });
}
