# Instruções do projeto

Este é um projeto **didático** dos padrões Inbox/Outbox com Kafka (Java + Spring Boot + Postgres).
Como serve de material de estudo, a documentação precisa refletir fielmente o código.

## Mantenha o README atualizado

**Sempre que uma mudança alterar o comportamento, a estrutura ou a forma de rodar a aplicação,
atualize o `README.md` na mesma alteração para refletir o estado atual.**

Isso inclui, por exemplo:

- Adicionar/remover/renomear módulos, serviços, endpoints ou tópicos Kafka.
- Mudar o esquema do banco (migrations Flyway), nomes de tabelas/colunas ou propriedades de config.
- Alterar dependências, versões (Java, Spring Boot, Kafka, etc.) ou a stack listada.
- Mudar como subir a infra (`docker-compose`), como executar os serviços ou como rodar os testes.
- Mudar o fluxo do exemplo (Outbox, Inbox, retry/DLT) ou a alternativa CDC (Debezium).

Ao atualizar, mantenha o README consistente: diagramas, tabela de arquivos-chave, comandos de
execução e a seção de stack devem continuar batendo com o código. Não deixe a doc divergir do
comportamento real — se não der para atualizar agora, sinalize explicitamente o que ficou pendente.
