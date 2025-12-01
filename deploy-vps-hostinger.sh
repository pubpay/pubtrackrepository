#!/bin/bash

# Script de Deploy Automatizado para VPS Hostinger
# Execute: bash deploy-vps-hostinger.sh

set -e  # Parar em caso de erro

# Cores
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Configurações
SERVER_IP="72.61.50.85"
SSH_USER="root"
APP_DIR="/var/www/leadrock-tracker"
GITHUB_REPO=""  # Preencha com: usuario/repositorio

echo -e "${YELLOW}🚀 Script de Deploy para VPS Hostinger${NC}"
echo -e "${YELLOW}===========================================${NC}"
echo ""

# Verificar se está conectado ao servidor
if [ ! -f "/etc/os-release" ] || ! grep -q "Ubuntu" /etc/os-release; then
    echo -e "${RED}❌ Este script deve ser executado no servidor Ubuntu${NC}"
    echo "Conecte-se primeiro: ssh root@72.61.50.85"
    exit 1
fi

echo -e "${GREEN}✅ Sistema detectado: Ubuntu${NC}"
echo ""

# 1. Atualizar sistema
echo -e "${YELLOW}1️⃣ Atualizando sistema...${NC}"
apt update && apt upgrade -y
echo -e "${GREEN}✅ Sistema atualizado${NC}"
echo ""

# 2. Instalar Node.js
echo -e "${YELLOW}2️⃣ Verificando Node.js...${NC}"
if ! command -v node &> /dev/null; then
    echo "Instalando Node.js 18.x..."
    curl -fsSL https://deb.nodesource.com/setup_18.x | bash -
    apt install -y nodejs
    echo -e "${GREEN}✅ Node.js instalado: $(node --version)${NC}"
else
    echo -e "${GREEN}✅ Node.js já instalado: $(node --version)${NC}"
fi
echo ""

# 3. Instalar PM2
echo -e "${YELLOW}3️⃣ Verificando PM2...${NC}"
if ! command -v pm2 &> /dev/null; then
    echo "Instalando PM2..."
    npm install -g pm2
    echo -e "${GREEN}✅ PM2 instalado${NC}"
else
    echo -e "${GREEN}✅ PM2 já instalado${NC}"
fi
echo ""

# 4. Criar diretório
echo -e "${YELLOW}4️⃣ Criando diretório do projeto...${NC}"
mkdir -p $APP_DIR
cd $APP_DIR
echo -e "${GREEN}✅ Diretório criado: $APP_DIR${NC}"
echo ""

# 5. Clonar ou atualizar projeto
echo -e "${YELLOW}5️⃣ Configurando projeto...${NC}"
if [ -d ".git" ]; then
    echo "Atualizando projeto existente..."
    git pull origin main || git pull origin master
    echo -e "${GREEN}✅ Projeto atualizado${NC}"
else
    if [ -z "$GITHUB_REPO" ]; then
        echo -e "${RED}⚠️  Repositório GitHub não configurado no script${NC}"
        echo "Por favor, clone manualmente:"
        echo "  git clone https://github.com/USUARIO/REPOSITORIO.git ."
        echo ""
        read -p "Pressione Enter após clonar o repositório..."
    else
        echo "Clonando repositório: $GITHUB_REPO"
        git clone https://github.com/$GITHUB_REPO.git .
        echo -e "${GREEN}✅ Projeto clonado${NC}"
    fi
fi
echo ""

# 6. Instalar dependências
echo -e "${YELLOW}6️⃣ Instalando dependências...${NC}"
npm install --production
echo -e "${GREEN}✅ Dependências instaladas${NC}"
echo ""

# 7. Criar diretório database
echo -e "${YELLOW}7️⃣ Criando diretório database...${NC}"
mkdir -p database
chmod 755 database
echo -e "${GREEN}✅ Diretório database criado${NC}"
echo ""

# 8. Parar aplicação antiga se existir
echo -e "${YELLOW}8️⃣ Parando aplicação antiga (se existir)...${NC}"
pm2 delete leadrock-tracker 2>/dev/null || true
echo -e "${GREEN}✅ Aplicação antiga removida${NC}"
echo ""

# 9. Iniciar aplicação
echo -e "${YELLOW}9️⃣ Iniciando aplicação com PM2...${NC}"
pm2 start server.js --name leadrock-tracker
pm2 save
echo -e "${GREEN}✅ Aplicação iniciada${NC}"
echo ""

# 10. Configurar PM2 startup
echo -e "${YELLOW}🔟 Configurando PM2 para iniciar automaticamente...${NC}"
STARTUP_CMD=$(pm2 startup systemd | grep -o "sudo.*")
if [ ! -z "$STARTUP_CMD" ]; then
    eval $STARTUP_CMD
    echo -e "${GREEN}✅ PM2 configurado para iniciar automaticamente${NC}"
else
    echo -e "${YELLOW}⚠️  Execute manualmente: pm2 startup systemd${NC}"
fi
echo ""

# 11. Configurar firewall
echo -e "${YELLOW}1️⃣1️⃣ Configurando firewall...${NC}"
if ! command -v ufw &> /dev/null; then
    apt install -y ufw
fi

# Permitir SSH primeiro
ufw allow 22/tcp 2>/dev/null || true
# Permitir porta da aplicação
ufw allow 3000/tcp 2>/dev/null || true
# Permitir HTTP/HTTPS
ufw allow 80/tcp 2>/dev/null || true
ufw allow 443/tcp 2>/dev/null || true

# Ativar firewall (não forçar se já estiver ativo)
ufw --force enable 2>/dev/null || true

echo -e "${GREEN}✅ Firewall configurado${NC}"
echo ""

# 12. Verificar status
echo -e "${YELLOW}1️⃣2️⃣ Verificando status da aplicação...${NC}"
pm2 status
echo ""

# 13. Mostrar logs
echo -e "${YELLOW}📊 Últimas linhas dos logs:${NC}"
pm2 logs leadrock-tracker --lines 20 --nostream
echo ""

# Resumo final
echo -e "${GREEN}===========================================${NC}"
echo -e "${GREEN}✅ DEPLOY CONCLUÍDO COM SUCESSO!${NC}"
echo -e "${GREEN}===========================================${NC}"
echo ""
echo -e "${YELLOW}📝 Informações:${NC}"
echo "  - Aplicação rodando em: http://$SERVER_IP:3000"
echo "  - Dashboard: http://$SERVER_IP:3000/dashboard"
echo "  - Postback: http://$SERVER_IP:3000/postback/lead"
echo ""
echo -e "${YELLOW}🔧 Comandos úteis:${NC}"
echo "  - Ver logs: pm2 logs leadrock-tracker"
echo "  - Reiniciar: pm2 restart leadrock-tracker"
echo "  - Status: pm2 status"
echo "  - Monitorar: pm2 monit"
echo ""
echo -e "${YELLOW}📦 Próximos passos (opcional):${NC}"
echo "  1. Configurar Nginx como proxy reverso"
echo "  2. Configurar SSL/HTTPS com Let's Encrypt"
echo "  3. Configurar backup automático do banco de dados"
echo ""

