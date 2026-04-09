# WhatsApp Pool - Multi-Instancia

Suporte a multiplos numeros WhatsApp alimentando o mesmo agente PicoClaw.

## Arquitetura

```
Celular A → whatsapp_1 (store.db/1) ──┐
Celular B → whatsapp_2 (store.db/2) ──┤→ MessageBus → Agente
Celular C → whatsapp_3 (store.db/3) ──┘
```

Cada slot tem seu proprio `store.db` com chaves de criptografia Signal independentes.

## Configuracao

No `config.json`:

```json
{
  "channels": {
    "whatsapp": {
      "enabled": true,
      "use_native": true,
      "pool": {
        "enabled": true,
        "max_slots": 10
      }
    }
  }
}
```

Ou desabilitar pool para modo single: `"pool": {"enabled": false}`

## Build

O pool requer a build tag `whatsapp_native`:

```bash
GO_BUILD_TAGS="goolm,stdjson,whatsapp_native" make build
cp build/picoclaw-darwin-arm64 picoclaw
```

## Parear um novo numero

**IMPORTANTE:** O gateway e o `wacli` nao podem acessar o mesmo store.db ao mesmo tempo.

```bash
# 1. Parar o gateway
pkill -f "picoclaw gateway"

# 2. Criar slot e gerar QR
mkdir -p ~/.picoclaw/workspace/whatsapp/1
WACLI_STORE_PATH="file:$HOME/.picoclaw/workspace/whatsapp/1/store.db?_foreign_keys=on" wacli auth

# 3. Escanear QR com o celular

# 4. MATAR wacli imediatamente apos parear
pkill -f "wacli auth"

# 5. Iniciar gateway
PICOCLAW_KEY_PASSPHRASE="..." ./picoclaw gateway
```

Para adicionar mais numeros, repetir com slot 2, 3, etc.

## QR Web App

Interface web para parear via navegador:

```bash
cd qr-code-picoclaw
WACLI_PATH=/Users/2a/.local/bin/wacli \
WACLI_STORE_BASE=/Users/2a/.picoclaw/workspace/whatsapp \
npx --package next@15.0.3 next dev -p 3001
```

Acesse http://localhost:3001 — mostra badges dos slots, QR code, e botao para remover slots.

## Verificar status

```bash
# Gateway ativo?
curl -s http://127.0.0.1:18790/health

# Slot autenticado?
WACLI_STORE_PATH="file:$HOME/.picoclaw/workspace/whatsapp/1/store.db?_foreign_keys=on" wacli auth status

# Numero de cada slot
sqlite3 ~/.picoclaw/workspace/whatsapp/1/store.db "SELECT jid FROM whatsmeow_device LIMIT 1;"
```

## Layout de arquivos

```
~/.picoclaw/workspace/whatsapp/
  1/store.db    ← whatsapp_1 (numero A)
  2/store.db    ← whatsapp_2 (numero B)
  3/store.db    ← whatsapp_3 (numero C)
  store.db      ← modo single (sem pool)
```

## Problemas conhecidos

| Problema | Causa | Solucao |
|----------|-------|---------|
| Agente nao responde | wacli e gateway acessando mesmo store.db | Matar wacli antes de iniciar gateway |
| "stream end frame" em loop | Slot vazio tentando conectar | Remover dirs de slots sem pareamento |
| "not compiled in" | Build sem tag | `GO_BUILD_TAGS="goolm,stdjson,whatsapp_native"` |
| QR invalido | store.db corrompido | Deletar dir do slot e parear novamente |

## Arquivos modificados

### PicoClaw (Go)
- `pkg/config/config.go` — `WhatsAppPoolConfig`
- `pkg/channels/whatsapp_native/pool.go` — Pool orchestrator
- `pkg/channels/whatsapp_native/whatsapp_native.go` — Hooks QRCallback, OnDisconnect, name param
- `pkg/channels/manager.go` — Skip single-instance quando pool ativo
- `pkg/gateway/gateway.go` — Pool init, API endpoints `/api/whatsapp/pool/status` e `/api/whatsapp/pool/qr`

### QR Code App (Next.js)
- `app/page.tsx` — Pool UI com badges, QR por slot, delete modal
- `app/api/whatsapp/pool/status/route.ts` — Status de todos slots com numero
- `app/api/whatsapp/pool/qr/route.ts` — SSE para QR por slot
- `app/api/whatsapp/pool/delete/route.ts` — Remover slot individual ou todos
