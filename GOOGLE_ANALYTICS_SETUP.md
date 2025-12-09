# Configuração do Google Analytics

Este documento explica como configurar o Google Analytics para substituir ou usar junto com o Clarity.

## Pré-requisitos

1. **Conta do Google Analytics 4 (GA4)**
   - Você precisa ter uma propriedade GA4 configurada
   - Anote o **Property ID** (ex: `123456789`)

2. **Service Account do Google Cloud**
   - Crie um projeto no [Google Cloud Console](https://console.cloud.google.com/)
   - Ative a API "Google Analytics Data API"
   - Crie uma Service Account e baixe o arquivo JSON de credenciais

## Configuração

### Opção 1: Variáveis de Ambiente (Recomendado)

Configure as seguintes variáveis de ambiente:

```bash
# ID da propriedade GA4 (apenas números, sem "GA4-")
GA_PROPERTY_ID=123456789

# Credenciais do Service Account (JSON como string ou caminho do arquivo)
GA_CREDENTIALS='{"type":"service_account","project_id":"...","private_key_id":"...","private_key":"...","client_email":"...","client_id":"...","auth_uri":"...","token_uri":"...","auth_provider_x509_cert_url":"...","client_x509_cert_url":"..."}'

# OU caminho para o arquivo JSON
GA_CREDENTIALS=/caminho/para/credentials.json

# URL base do site (opcional, padrão: https://news.wellhubus.com)
GA_BASE_URL=https://news.wellhubus.com
```

### Opção 2: Arquivo .env

Crie um arquivo `.env` na raiz do projeto:

```env
GA_PROPERTY_ID=123456789
GA_CREDENTIALS={"type":"service_account",...}
GA_BASE_URL=https://news.wellhubus.com
```

**Nota:** Se usar arquivo `.env`, você precisará instalar o pacote `dotenv`:
```bash
npm install dotenv
```

E adicionar no início do `server.js`:
```javascript
require('dotenv').config();
```

## Permissões do Service Account

Certifique-se de que a Service Account tem as seguintes permissões no Google Analytics:

1. Acesse o Google Analytics
2. Vá em **Admin** > **Property Access Management**
3. Adicione o email da Service Account (ex: `service-account@project.iam.gserviceaccount.com`)
4. Dê permissão de **Viewer** ou **Analyst**

## Como Usar

### 1. Verificar Status

```bash
GET /api/analytics/status
```

Retorna se o Google Analytics está configurado corretamente.

### 2. Atualizar Dados

```bash
POST /api/analytics/update
Content-Type: application/json

{
  "numOfDays": 1
}
```

Isso buscará dados do Google Analytics e salvará na mesma tabela `clarity_data` (para compatibilidade).

## Diferenças entre Clarity e Google Analytics

| Recurso | Clarity | Google Analytics |
|---------|---------|------------------|
| Limite de registros | 999 por requisição | 100.000 por requisição |
| Métricas | totalSessionCount, distantUserCount | screenPageViews, activeUsers |
| Autenticação | Token Bearer | Service Account (OAuth2) |
| Dados armazenados | Mesma tabela `clarity_data` | Mesma tabela `clarity_data` |

## Migração do Clarity para Google Analytics

1. Configure as variáveis de ambiente do Google Analytics
2. Use a rota `/api/analytics/update` em vez de `/api/clarity/update`
3. Os dados serão salvos na mesma tabela, então o código existente continuará funcionando
4. Você pode usar ambos simultaneamente se desejar

## Solução de Problemas

### Erro: "Cliente do Google Analytics não disponível"
- Verifique se `GA_CREDENTIALS` está configurado corretamente
- Verifique se o arquivo JSON de credenciais existe (se usar caminho de arquivo)

### Erro: "ID da propriedade do Google Analytics não configurado"
- Verifique se `GA_PROPERTY_ID` está configurado
- O ID deve ser apenas números (sem "GA4-" ou outros prefixos)

### Erro: "Permission denied"
- Verifique se a Service Account tem permissão no Google Analytics
- Verifique se a API "Google Analytics Data API" está ativada no Google Cloud

### Dados não aparecem
- Verifique se há dados no Google Analytics para o período selecionado
- Verifique os logs do servidor para ver se há erros
- Verifique se as URLs estão sendo extraídas corretamente

