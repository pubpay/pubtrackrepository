# 🚀 Guia de Deploy na VPS Hostinger - Ubuntu 22.04

## 📋 Informações da VPS
- **IP:** 72.61.50.85
- **Usuário:** root
- **Senha:** u&ohBxd9n8nhostinger
- **Sistema:** Ubuntu 22.04

---

## 🔧 Passo 1: Conectar na VPS

```bash
ssh root@72.61.50.85
# Digite a senha quando solicitado
```

---

## 📦 Passo 2: Atualizar o Sistema

```bash
apt update && apt upgrade -y
```

---

## 🟢 Passo 3: Instalar Node.js (versão 18 LTS)

```bash
# Instalar Node.js 18.x
curl -fsSL https://deb.nodesource.com/setup_18.x | bash -
apt install -y nodejs

# Verificar instalação
node --version
npm --version
```

---

## 🔄 Passo 4: Instalar PM2 (Gerenciador de Processos)

```bash
npm install -g pm2

# Configurar PM2 para iniciar automaticamente
pm2 startup systemd
# Execute o comando que aparecer na tela (algo como: sudo env PATH=...)
```

---

## 📁 Passo 5: Criar Diretório do Projeto

```bash
mkdir -p /var/www/leadrock-tracker
cd /var/www/leadrock-tracker
```

---

## 🔐 Passo 6: Clonar Projeto do GitHub

```bash
# Se seu repositório é privado, você precisará configurar SSH key ou usar token
# Opção 1: Clonar via HTTPS (vai pedir usuário/senha do GitHub)
git clone https://github.com/SEU_USUARIO/SEU_REPOSITORIO.git .

# Opção 2: Clonar via SSH (recomendado - precisa configurar SSH key antes)
# git clone git@github.com:SEU_USUARIO/SEU_REPOSITORIO.git .

# Se já tem o projeto local, você pode enviar via SCP (do seu computador):
# scp -r * root@72.61.50.85:/var/www/leadrock-tracker/
```

---

## 📥 Passo 7: Instalar Dependências

```bash
cd /var/www/leadrock-tracker
npm install --production
```

---

## 🗄️ Passo 8: Criar Diretório do Banco de Dados

```bash
mkdir -p database
chmod 755 database
```

---

## ⚙️ Passo 9: Configurar Variáveis de Ambiente (Opcional)

```bash
# Criar arquivo .env se necessário
nano .env
```

Se seu projeto usa variáveis de ambiente, adicione:
```
PORT=3000
NODE_ENV=production
```

---

## 🚀 Passo 10: Iniciar Aplicação com PM2

```bash
cd /var/www/leadrock-tracker

# Iniciar aplicação
pm2 start server.js --name leadrock-tracker

# Salvar configuração do PM2
pm2 save

# Verificar status
pm2 status
pm2 logs leadrock-tracker
```

---

## 🔥 Passo 11: Configurar Firewall (UFW)

```bash
# Instalar UFW se não estiver instalado
apt install -y ufw

# Permitir SSH (IMPORTANTE - faça isso primeiro!)
ufw allow 22/tcp

# Permitir porta da aplicação
ufw allow 3000/tcp

# Ativar firewall
ufw enable

# Verificar status
ufw status
```

---

## 🌐 Passo 12: Configurar Nginx como Proxy Reverso (Opcional mas Recomendado)

### 12.1 Instalar Nginx

```bash
apt install -y nginx
```

### 12.2 Criar Configuração do Site

```bash
nano /etc/nginx/sites-available/leadrock-tracker
```

Cole o seguinte conteúdo (substitua `SEU_DOMINIO.com` pelo seu domínio ou use o IP):

```nginx
server {
    listen 80;
    server_name 72.61.50.85;  # ou seu dominio.com

    location / {
        proxy_pass http://localhost:3000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_cache_bypass $http_upgrade;
    }
}
```

### 12.3 Ativar Site

```bash
# Criar link simbólico
ln -s /etc/nginx/sites-available/leadrock-tracker /etc/nginx/sites-enabled/

# Remover configuração padrão (opcional)
rm /etc/nginx/sites-enabled/default

# Testar configuração
nginx -t

# Reiniciar Nginx
systemctl restart nginx
systemctl enable nginx
```

### 12.4 Permitir Porta 80 no Firewall

```bash
ufw allow 80/tcp
ufw allow 443/tcp  # Para HTTPS futuro
```

---

## ✅ Passo 13: Verificar se Está Funcionando

```bash
# Ver logs do PM2
pm2 logs leadrock-tracker

# Ver status
pm2 status

# Testar localmente no servidor
curl http://localhost:3000

# Testar via IP externo (do seu computador)
# Acesse: http://72.61.50.85:3000
# Ou se configurou Nginx: http://72.61.50.85
```

---

## 🔄 Comandos Úteis para Manutenção

### Atualizar Aplicação (quando fizer push no GitHub)

```bash
cd /var/www/leadrock-tracker
git pull origin main  # ou master, dependendo da branch
npm install --production
pm2 restart leadrock-tracker
```

### Ver Logs

```bash
# Logs em tempo real
pm2 logs leadrock-tracker

# Últimas 100 linhas
pm2 logs leadrock-tracker --lines 100

# Apenas erros
pm2 logs leadrock-tracker --err
```

### Reiniciar Aplicação

```bash
pm2 restart leadrock-tracker
```

### Parar Aplicação

```bash
pm2 stop leadrock-tracker
```

### Deletar Aplicação do PM2

```bash
pm2 delete leadrock-tracker
```

### Monitorar Recursos

```bash
pm2 monit
```

---

## 🔒 Passo 14: Configurar SSL/HTTPS (Opcional mas Recomendado)

### Instalar Certbot

```bash
apt install -y certbot python3-certbot-nginx
```

### Obter Certificado SSL

```bash
# Se você tem um domínio apontando para o IP
certbot --nginx -d seu-dominio.com

# Ou se quiser usar apenas IP (não recomendado, mas possível)
# Você precisará usar outro método como Cloudflare ou Let's Encrypt via DNS
```

---

## 📊 Monitoramento e Backup

### Backup do Banco de Dados

```bash
# Criar script de backup
nano /root/backup-database.sh
```

Cole:

```bash
#!/bin/bash
BACKUP_DIR="/root/backups"
DATE=$(date +%Y%m%d_%H%M%S)
mkdir -p $BACKUP_DIR
cp /var/www/leadrock-tracker/database/data.db $BACKUP_DIR/data_$DATE.db
# Manter apenas últimos 7 backups
find $BACKUP_DIR -name "data_*.db" -mtime +7 -delete
```

```bash
chmod +x /root/backup-database.sh

# Agendar backup diário (crontab)
crontab -e
# Adicione: 0 2 * * * /root/backup-database.sh
```

---

## ⚠️ Troubleshooting

### Aplicação não inicia

```bash
# Ver logs detalhados
pm2 logs leadrock-tracker --err

# Verificar se a porta está em uso
netstat -tulpn | grep 3000

# Verificar permissões
ls -la /var/www/leadrock-tracker/database
```

### Erro de permissão no banco de dados

```bash
chown -R root:root /var/www/leadrock-tracker
chmod -R 755 /var/www/leadrock-tracker
chmod -R 777 /var/www/leadrock-tracker/database
```

### Nginx não funciona

```bash
# Ver logs do Nginx
tail -f /var/log/nginx/error.log

# Testar configuração
nginx -t

# Reiniciar
systemctl restart nginx
```

### Porta 3000 não acessível externamente

```bash
# Verificar firewall
ufw status

# Verificar se aplicação está rodando
pm2 status

# Testar localmente
curl http://localhost:3000
```

---

## 🎯 URLs Finais

Após o deploy completo:

- **Sem Nginx:** http://72.61.50.85:3000
- **Com Nginx:** http://72.61.50.85
- **Dashboard:** http://72.61.50.85/dashboard
- **Postback:** http://72.61.50.85/postback/lead

---

## 📝 Notas Importantes

1. **Segurança:**
   - Mude a senha do root após o primeiro acesso
   - Configure chaves SSH ao invés de senha
   - Mantenha o sistema atualizado: `apt update && apt upgrade`

2. **Performance:**
   - O PM2 gerencia automaticamente a aplicação
   - Configure múltiplas instâncias se necessário: `pm2 start server.js -i 2`

3. **Backup:**
   - Faça backup regular do banco de dados SQLite
   - Considere usar MongoDB para produção (veja seção abaixo)

4. **Domínio:**
   - Configure DNS apontando para o IP 72.61.50.85
   - Use Nginx para servir na porta 80/443

---

## 🔄 Migração para MongoDB (Opcional - se quiser MEAN Stack completo)

Se você realmente quiser usar MongoDB ao invés de SQLite:

### Instalar MongoDB

```bash
# Importar chave GPG
curl -fsSL https://www.mongodb.org/static/pgp/server-6.0.asc | gpg -o /usr/share/keyrings/mongodb-server-6.0.gpg --dearmor

# Adicionar repositório
echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-6.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/6.0 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-6.0.list

# Instalar
apt update
apt install -y mongodb-org

# Iniciar MongoDB
systemctl start mongod
systemctl enable mongod
```

### Migrar código para usar MongoDB

Você precisaria modificar o `server.js` para usar MongoDB ao invés de SQLite. Mas isso é uma mudança significativa no código.

**Recomendação:** SQLite funciona perfeitamente para este projeto. MongoDB só é necessário se você precisar de:
- Escalabilidade horizontal
- Replicação de dados
- Múltiplos servidores

---

## ✅ Checklist Final

- [ ] Node.js instalado
- [ ] PM2 instalado e configurado
- [ ] Projeto clonado do GitHub
- [ ] Dependências instaladas
- [ ] Aplicação rodando com PM2
- [ ] Firewall configurado
- [ ] Nginx configurado (opcional)
- [ ] Aplicação acessível via IP
- [ ] Backup configurado
- [ ] SSL configurado (opcional)

---

**🎉 Pronto! Seu projeto está no ar!**

