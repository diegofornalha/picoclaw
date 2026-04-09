# WhatsApp Groups Reference

Mapeamento de grupos WhatsApp identificados via JID.

## Grupos Identificados

| JID | Nome | Status |
|-----|------|--------|
| `120363419962423658@g.us` | Acervo (avisos) MENTORIA | Ativo |

## Como Identificar Novos Grupos

1. Executar o teste que envia "oi" para o grupo desconhecido
2. Usuário identifica qual grupo recebeu a mensagem
3. Adicionar o mapeamento nesta tabela

## Referência Técnica

- Database: `/Users/2a/.picoclaw/workspace/whatsapp/store.db`
- Query para listar todos: `SELECT DISTINCT chat_jid FROM whatsmeow_chat_settings WHERE chat_jid LIKE '%@g.us'`
- Test program: `/Users/2a/.claude/test_poll.go`
