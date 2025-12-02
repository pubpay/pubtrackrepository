// Arquivo de configuração PM2 para Hostinger VPS
module.exports = {
  apps: [{
    name: 'leadrock-tracker',
    script: 'server.js',
    instances: 1,
    autorestart: true,
    watch: false,
    max_memory_restart: '1G',
    env: {
      NODE_ENV: 'production',
      PORT: 3000
    }
  }]
};

