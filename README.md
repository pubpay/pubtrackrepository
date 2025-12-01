# 📊 LeadRock Postback Tracker

Sistema completo para receber e exibir postbacks da LeadRock em tempo real.

## 🚀 Instalação

1. **Instalar dependências:**
```bash
npm install
```

## ▶️ Como Rodar

Execute o comando:
```bash
npm start
```

O servidor será iniciado em `http://localhost:3000`

## 📍 URLs Importantes

- **Dashboard:** http://localhost:3000/dashboard
- **Postback URL:** http://localhost:3000/postback

## 📥 Exemplo de Postback

A LeadRock enviará dados via GET para a rota `/postback` com os seguintes parâmetros:

```
http://localhost:3000/postback?sub_id1=Campanha1&sub_id2=ConjuntoA&sub_id3=AnuncioMulher&offer_id=123&status=FTD&payout=25.00&date=2025-11-08
```

### Parâmetros:
- `sub_id1` - Campanha
- `sub_id2` - Conjunto
- `sub_id3` - Anúncio
- `offer_id` - ID da oferta
- `status` - Status da conversão (FTD, DEPOSIT, etc.)
- `payout` - Valor da conversão
- `date` - Data da conversão

## 🎯 Funcionalidades

✅ Recebe postbacks via GET  
✅ Salva dados automaticamente no SQLite  
✅ Dashboard em tempo real  
✅ Atualização automática a cada 10 segundos  
✅ Notificação visual quando novo postback é recebido  
✅ Estatísticas de conversões e valores  
✅ Interface moderna com TailwindCSS  

## 📁 Estrutura do Projeto

```
.
├── server.js              # Servidor Express principal
├── package.json           # Dependências do projeto
├── database/
│   └── data.db           # Banco de dados SQLite (criado automaticamente)
└── public/
    └── index.html        # Dashboard frontend
```

## 🗄️ Banco de Dados

O banco de dados SQLite é criado automaticamente na primeira execução em `database/data.db`.

**Tabela: `conversions`**
- `id` - ID único (auto-incremento)
- `sub_id1` - Campanha
- `sub_id2` - Conjunto
- `sub_id3` - Anúncio
- `offer_id` - ID da oferta
- `status` - Status da conversão
- `payout` - Valor da conversão
- `date` - Data da conversão
- `created_at` - Timestamp de criação (automático)

## 📝 Logs

O servidor exibe logs no terminal sempre que um postback é recebido, mostrando todos os dados recebidos.

## 🔧 Tecnologias Utilizadas

- **Node.js** - Runtime JavaScript
- **Express** - Framework web
- **SQLite3** - Banco de dados
- **TailwindCSS** - Framework CSS
- **JavaScript (Vanilla)** - Frontend

## 🌐 Hospedagem

### 🏆 Hostinger (Se você já tem)
Se você tem **VPS ou Cloud Hosting** na Hostinger, consulte **`HOSPEDAGEM_HOSTINGER.md`** para guia completo!

**Vantagens da Hostinger:**
- ✅ Controle total (VPS)
- ✅ Melhor performance
- ✅ Domínio próprio fácil
- ✅ Suporte em português

### Outras Opções
Consulte o arquivo **`HOSPEDAGEM.md`** para:
- **Render.com** (Recomendado - Grátis)
- **Railway.app** (Recomendado - Grátis)
- **Fly.io** (Grátis)

### Resumo Rápido (Render.com):
1. Crie conta no GitHub e faça upload do projeto
2. Acesse https://render.com e conecte seu repositório
3. Configure:
   - Build Command: `npm install`
   - Start Command: `npm start`
4. Deploy automático! 🚀

## 📌 Notas

- O banco de dados é criado automaticamente na primeira execução
- Os dados são ordenados por data de criação (mais recentes primeiro)
- A atualização automática acontece a cada 10 segundos
- O alerta verde aparece por 5 segundos quando um novo postback é recebido
- Para hospedagem gratuita, use serviços de ping (ex: UptimeRobot) para manter o servidor ativo

