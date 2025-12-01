# ⚡ Comandos Rápidos - Deploy VPS Hostinger

## 🔐 Conectar na VPS

```bash
ssh root@72.61.50.85
# Senha: u&ohBxd9n8nhostinger
```

---

## 🚀 Deploy Inicial (Primeira Vez)

### Opção 1: Script Automatizado

```bash
# No servidor, baixe e execute o script
wget https://raw.githubusercontent.com/SEU_USUARIO/SEU_REPO/main/deploy-vps-hostinger.sh
chmod +x deploy-vps-hostinger.sh
bash deploy-vps-hostinger.sh
```

### Opção 2: Manual (Passo a Passo)

```bash
# 1. Atualizar sistema
apt update && apt upgrade -y

# 2. Instalar Node.js
curl -fsSL https://deb.nodesource.com/setup_18.x | bash -
apt install -y nodejs

# 3. Instalar PM2
npm install -g pm2

# 4. Criar diretório
mkdir -p /var/www/leadrock-tracker
cd /var/www/leadrock-tracker

# 5. Clonar projeto (substitua pela URL do seu repositório)
git clone https://github.com/SEU_USUARIO/SEU_REPOSITORIO.git .

# 6. Instalar dependências
npm install --production

# 7. Criar diretório database
mkdir -p database
chmod 755 database

# 8. Iniciar aplicação
pm2 start server.js --name leadrock-tracker
pm2 save
pm2 startup systemd  # Execute o comando que aparecer

# 9. Configurar firewall
ufw allow 22/tcp
ufw allow 3000/tcp
ufw enable
```

---

## 🔄 Atualizar Aplicação (Após Push no GitHub)

```bash
cd /var/www/leadrock-tracker
git pull origin main  # ou master
npm install --production
pm2 restart leadrock-tracker
```

---

## 📊 Comandos PM2

```bash
# Ver status
pm2 status

# Ver logs em tempo real
pm2 logs leadrock-tracker

# Últimas 100 linhas
pm2 logs leadrock-tracker --lines 100

# Apenas erros
pm2 logs leadrock-tracker --err

# Reiniciar
pm2 restart leadrock-tracker

# Parar
pm2 stop leadrock-tracker

# Deletar
pm2 delete leadrock-tracker

# Monitorar recursos
pm2 monit

# Salvar configuração
pm2 save
```

---

## 🔥 Firewall (UFW)

```bash
# Ver status
ufw status

# Permitir porta
ufw allow 3000/tcp

# Permitir HTTP/HTTPS
ufw allow 80/tcp
ufw allow 443/tcp

# Ativar/Desativar
ufw enable
ufw disable
```

---

## 🌐 Nginx (Proxy Reverso)

### Instalar

```bash
apt install -y nginx
```

### Criar Configuração

```bash
nano /etc/nginx/sites-available/leadrock-tracker
```

Cole:

```nginx
server {
    listen 80;
    server_name 72.61.50.85;

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

### Ativar

```bash
ln -s /etc/nginx/sites-available/leadrock-tracker /etc/nginx/sites-enabled/
nginx -t
systemctl restart nginx
```

---

## 🔒 SSL/HTTPS (Let's Encrypt)

```bash
# Instalar Certbot
apt install -y certbot python3-certbot-nginx

# Obter certificado (se tiver domínio)
certbot --nginx -d seu-dominio.com

# Renovar automaticamente (já vem configurado)
certbot renew --dry-run
```

---

## 🗄️ Backup do Banco de Dados

```bash
# Backup manual
cp /var/www/leadrock-tracker/database/data.db /root/backup_$(date +%Y%m%d).db

# Backup automático (adicionar ao crontab)
crontab -e
# Adicionar: 0 2 * * * cp /var/www/leadrock-tracker/database/data.db /root/backups/data_$(date +\%Y\%m\%d).db
```

---

## 🐛 Troubleshooting

### Aplicação não inicia

```bash
# Ver logs
pm2 logs leadrock-tracker --err

# Verificar porta
netstat -tulpn | grep 3000

# Verificar permissões
ls -la /var/www/leadrock-tracker/database
```

### Erro de permissão

```bash
chmod -R 755 /var/www/leadrock-tracker
chmod -R 777 /var/www/leadrock-tracker/database
```

### Nginx não funciona

```bash
# Ver logs
tail -f /var/log/nginx/error.log

# Testar configuração
nginx -t

# Reiniciar
systemctl restart nginx
```

---

## 📍 URLs

- **Aplicação:** http://72.61.50.85:3000
- **Com Nginx:** http://72.61.50.85
- **Dashboard:** http://72.61.50.85/dashboard
- **Postback:** http://72.61.50.85/postback/lead

---

## ✅ Checklist Rápido

- [ ] Node.js instalado
- [ ] PM2 instalado
- [ ] Projeto clonado
- [ ] Dependências instaladas
- [ ] Aplicação rodando
- [ ] Firewall configurado
- [ ] Nginx configurado (opcional)
- [ ] SSL configurado (opcional)

