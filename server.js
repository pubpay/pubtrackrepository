const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');
const cors = require('cors');
const https = require('https');
const { google } = require('googleapis');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// Middleware para log de TODAS as requisições (diagnóstico)
app.use((req, res, next) => {
  if (req.path.includes('/postback')) {
    console.log(`\n🔔 REQUISIÇÃO RECEBIDA: ${req.method} ${req.path}`);
    console.log('  - Query params:', JSON.stringify(req.query));
    console.log('  - IP:', req.ip || req.connection.remoteAddress);
    console.log('  - User-Agent:', req.get('user-agent') || 'N/A');
  }
  next();
});

// Criar pasta database se não existir
const databaseDir = path.join(__dirname, 'database');
console.log('📁 Diretório do projeto:', __dirname);
console.log('📁 Tentando criar/verificar pasta database:', databaseDir);

try {
  if (!fs.existsSync(databaseDir)) {
    fs.mkdirSync(databaseDir, { recursive: true });
    console.log('✅ Pasta database criada:', databaseDir);
  } else {
    console.log('✅ Pasta database já existe:', databaseDir);
  }
  
  // Verificar permissões
  const stats = fs.statSync(databaseDir);
  console.log('📊 Permissões da pasta:', stats.mode.toString(8));
  console.log('📊 É diretório?', stats.isDirectory());
  
  // Testar se podemos escrever na pasta
  const testFile = path.join(databaseDir, '.test-write');
  try {
    fs.writeFileSync(testFile, 'test');
    fs.unlinkSync(testFile);
    console.log('✅ Pasta tem permissão de escrita');
  } catch (writeErr) {
    console.error('❌ Pasta NÃO tem permissão de escrita:', writeErr.message);
  }
} catch (err) {
  console.error('❌ Erro ao criar/verificar pasta database:', err.message);
  console.error('Stack:', err.stack);
}

// Caminho do banco de dados
const dbPath = path.join(__dirname, 'database', 'data.db');
console.log('📁 Caminho completo do banco:', dbPath);

// Verificar se o diretório pai existe antes de criar o banco
if (!fs.existsSync(databaseDir)) {
  console.error('❌ Diretório database não existe após tentativa de criação!');
}

// Inicializar banco de dados com modo de escrita
let db;
try {
  // Tentar criar o arquivo vazio primeiro para garantir permissões
  if (!fs.existsSync(dbPath)) {
    try {
      fs.writeFileSync(dbPath, '');
      console.log('✅ Arquivo data.db criado com sucesso');
    } catch (fileErr) {
      console.error('❌ Erro ao criar arquivo data.db:', fileErr.message);
    }
  }
  
  db = new sqlite3.Database(dbPath, sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE, (err) => {
    if (err) {
      console.error('❌ Erro ao conectar ao banco de dados:', err.message);
      console.error('Caminho tentado:', dbPath);
      console.error('Diretório existe?', fs.existsSync(databaseDir));
      if (fs.existsSync(databaseDir)) {
        try {
          const stats = fs.statSync(databaseDir);
          console.error('Permissões do diretório:', stats.mode.toString(8));
          console.error('É diretório?', stats.isDirectory());
        } catch (statErr) {
          console.error('Erro ao verificar permissões:', statErr.message);
        }
      }
      // Tentar verificar se o arquivo existe
      console.error('Arquivo data.db existe?', fs.existsSync(dbPath));
    } else {
      console.log('✅ Conectado ao banco de dados SQLite');
      console.log('📁 Caminho do banco:', dbPath);
      
      // Configurar SQLite para não limitar resultados (remover qualquer limite padrão)
      // SQLite não tem limite padrão de 999, mas vamos garantir que não há limitações
      db.run("PRAGMA page_size = 4096", (err) => {
        if (err) console.error('❌ Erro ao configurar page_size:', err.message);
      });
      
      // Criar tabelas se não existirem
      db.run(`CREATE TABLE IF NOT EXISTS conversions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sub_id1 TEXT,
        sub_id2 TEXT,
        sub_id3 TEXT,
        sub_id4 TEXT,
        sub_id5 TEXT,
        sub_id6 TEXT,
        sub_id7 TEXT,
        sub_id8 TEXT,
        campanha TEXT,
        conjunto TEXT,
        anuncio TEXT,
        offer_id TEXT,
        lead_id TEXT,
        status TEXT,
        payout REAL,
        date TEXT,
        notification_type TEXT,
        utm_source TEXT,
        utm_medium TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )`, (err) => {
        if (err) {
          console.error('❌ Erro ao criar tabela conversions:', err.message);
        } else {
          console.log('✅ Tabela conversions criada/verificada');
          
          // Adicionar colunas campanha, conjunto, anuncio se não existirem (migração)
          db.all("PRAGMA table_info(conversions)", [], (err, columns) => {
            if (err) {
              console.error('❌ Erro ao verificar colunas:', err.message);
              return;
            }
            
            const columnNames = columns.map(col => col.name);
            const columnsToAdd = [];
            
            if (!columnNames.includes('campanha')) {
              columnsToAdd.push('campanha TEXT');
            }
            if (!columnNames.includes('conjunto')) {
              columnsToAdd.push('conjunto TEXT');
            }
            if (!columnNames.includes('anuncio')) {
              columnsToAdd.push('anuncio TEXT');
            }
            if (!columnNames.includes('lead_id')) {
              columnsToAdd.push('lead_id TEXT');
            }
            if (!columnNames.includes('categoria')) {
              columnsToAdd.push('categoria TEXT');
            }
            
            if (columnsToAdd.length > 0) {
              columnsToAdd.forEach(colDef => {
                const colName = colDef.split(' ')[0];
                db.run(`ALTER TABLE conversions ADD COLUMN ${colDef}`, (err) => {
                  if (err) {
                    console.error(`❌ Erro ao adicionar coluna ${colName}:`, err.message);
                  } else {
                    console.log(`✅ Coluna ${colName} adicionada com sucesso`);
                  }
                });
              });
            } else {
              console.log('✅ Todas as colunas necessárias já existem');
            }
          });
        }
      });

      // Criar tabela de produtos cadastrados
      db.run(`CREATE TABLE IF NOT EXISTS produtos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome_produto TEXT NOT NULL,
        offer_id TEXT NOT NULL,
        nome_conta TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )`, (err) => {
        if (err) {
          console.error('❌ Erro ao criar tabela produtos:', err.message);
        } else {
          console.log('✅ Tabela produtos criada/verificada');
          
          // Migração: Remover constraint UNIQUE do offer_id se existir
          db.all("SELECT sql FROM sqlite_master WHERE type='table' AND name='produtos'", [], (err, rows) => {
            if (err) {
              console.error('❌ Erro ao verificar estrutura da tabela produtos:', err.message);
              return;
            }
            
            if (rows.length > 0) {
              const createTableSql = rows[0].sql || '';
              // Verificar se há constraint UNIQUE no offer_id
              if (createTableSql.includes('offer_id TEXT NOT NULL UNIQUE') || createTableSql.includes('offer_id TEXT UNIQUE')) {
                console.log('🔄 Migrando tabela produtos: removendo constraint UNIQUE do offer_id...');
                
                // Criar nova tabela sem a constraint UNIQUE
                db.run(`CREATE TABLE produtos_new (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  nome_produto TEXT NOT NULL,
                  offer_id TEXT NOT NULL,
                  nome_conta TEXT NOT NULL,
                  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )`, (err) => {
                  if (err) {
                    console.error('❌ Erro ao criar tabela produtos_new:', err.message);
                    return;
                  }
                  
                  // Copiar dados da tabela antiga para a nova
                  db.run(`INSERT INTO produtos_new (id, nome_produto, offer_id, nome_conta, created_at, updated_at)
                          SELECT id, nome_produto, offer_id, nome_conta, created_at, updated_at FROM produtos`, (err) => {
                    if (err) {
                      console.error('❌ Erro ao copiar dados:', err.message);
                      // Remover tabela nova em caso de erro
                      db.run('DROP TABLE produtos_new');
                      return;
                    }
                    
                    // Remover tabela antiga
                    db.run('DROP TABLE produtos', (err) => {
                      if (err) {
                        console.error('❌ Erro ao remover tabela antiga:', err.message);
                        return;
                      }
                      
                      // Renomear tabela nova
                      db.run('ALTER TABLE produtos_new RENAME TO produtos', (err) => {
                        if (err) {
                          console.error('❌ Erro ao renomear tabela:', err.message);
                        } else {
                          console.log('✅ Migração concluída: constraint UNIQUE removida do offer_id');
                        }
                      });
                    });
                  });
                });
              } else {
                console.log('✅ Tabela produtos já está sem constraint UNIQUE no offer_id');
              }
            }
          });
        }
      });

      // Criar tabela de estatísticas por campanha
      db.run(`CREATE TABLE IF NOT EXISTS campaign_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        campanha TEXT,
        campanha_id TEXT,
        conjunto TEXT,
        conjunto_id TEXT,
        anuncio TEXT,
        anuncio_id TEXT,
        placement TEXT,
        site_source TEXT,
        leads INTEGER DEFAULT 0,
        conversoes INTEGER DEFAULT 0,
        trash INTEGER DEFAULT 0,
        cancel INTEGER DEFAULT 0,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(campanha, conjunto, anuncio)
      )`, (err) => {
        if (err) {
          console.error('❌ Erro ao criar tabela campaign_stats:', err.message);
        } else {
          console.log('✅ Tabela campaign_stats criada/verificada');
        }
      });

      // Criar tabela para armazenar dados do Clarity
      db.run(`CREATE TABLE IF NOT EXISTS clarity_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT NOT NULL,
        sub2 TEXT,
        acessos INTEGER DEFAULT 0,
        usuarios_unicos INTEGER DEFAULT 0,
        data_coleta DATE NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(url, data_coleta)
      )`, (err) => {
        if (err) {
          console.error('❌ Erro ao criar tabela clarity_data:', err.message);
        } else {
          console.log('✅ Tabela clarity_data criada/verificada');
          
          // Adicionar coluna usuarios_unicos se não existir (migração)
          db.all("PRAGMA table_info(clarity_data)", [], (err, columns) => {
            if (err) {
              console.error('❌ Erro ao verificar colunas:', err.message);
              return;
            }
            
            const columnNames = columns.map(col => col.name);
            if (!columnNames.includes('usuarios_unicos')) {
              db.run('ALTER TABLE clarity_data ADD COLUMN usuarios_unicos INTEGER DEFAULT 0', (err) => {
                if (err) {
                  console.error('❌ Erro ao adicionar coluna usuarios_unicos:', err.message);
                } else {
                  console.log('✅ Coluna usuarios_unicos adicionada com sucesso');
                }
              });
            }
          });
        }
      });

      // Criar tabela para controlar requisições do Clarity
      db.run(`CREATE TABLE IF NOT EXISTS clarity_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data DATE NOT NULL UNIQUE,
        requisicoes_feitas INTEGER DEFAULT 0,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )`, (err) => {
        if (err) {
          console.error('❌ Erro ao criar tabela clarity_requests:', err.message);
        } else {
          console.log('✅ Tabela clarity_requests criada/verificada');
        }
      });
    }
  });
} catch (dbErr) {
  console.error('❌ Erro ao inicializar banco de dados:', dbErr.message);
  console.error('Stack:', dbErr.stack);
  // Criar um banco "mock" para não quebrar a aplicação
  db = null;
}

// Função auxiliar para obter data/hora atual no formato SQL (YYYY-MM-DD HH:MM:SS) no fuso do Brasil
function getBrazilDateTimeSQL() {
  const now = new Date();
  // Usar Intl.DateTimeFormat para obter componentes no fuso horário do Brasil
  const formatter = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/Sao_Paulo',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false
  });
  
  const parts = formatter.formatToParts(now);
  const year = parts.find(p => p.type === 'year').value;
  const month = parts.find(p => p.type === 'month').value;
  const day = parts.find(p => p.type === 'day').value;
  const hours = parts.find(p => p.type === 'hour').value;
  const minutes = parts.find(p => p.type === 'minute').value;
  const seconds = parts.find(p => p.type === 'second').value;
  
  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

// Função auxiliar para obter data/hora atual no formato SQL (YYYY-MM-DD HH:MM:SS) no fuso do México
// NOTA: Esta função não está mais sendo usada. Mantida apenas para referência futura.
// Atualmente, todas as datas e timestamps usam o horário de São Paulo/Brasil.
function getMexicoDateTimeSQL() {
  const now = new Date();
  // Usar Intl.DateTimeFormat para obter componentes no fuso horário do México
  const formatter = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/Mexico_City',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false
  });
  
  const parts = formatter.formatToParts(now);
  const year = parts.find(p => p.type === 'year').value;
  const month = parts.find(p => p.type === 'month').value;
  const day = parts.find(p => p.type === 'day').value;
  const hours = parts.find(p => p.type === 'hour').value;
  const minutes = parts.find(p => p.type === 'minute').value;
  const seconds = parts.find(p => p.type === 'second').value;
  
  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

// Função auxiliar para obter data atual no formato YYYY-MM-DD baseada no horário de São Paulo/Brasil
function getTodayDate() {
  // Usar fuso horário do Brasil (America/Sao_Paulo) para determinar a data
  const now = new Date();
  const brazilTime = new Date(now.toLocaleString('en-US', {timeZone: 'America/Sao_Paulo'}));
  const year = brazilTime.getFullYear();
  const month = String(brazilTime.getMonth() + 1).padStart(2, '0');
  const day = String(brazilTime.getDate()).padStart(2, '0');
  const todayStr = `${year}-${month}-${day}`;
  return todayStr;
}

// Função auxiliar para converter uma data/hora para o fuso horário de São Paulo/Brasil e retornar apenas a data (YYYY-MM-DD)
// Usada quando recebe uma data no postback e precisa determinar qual dia é no Brasil
function getDateInBrazilTimezone(dateInput) {
  if (!dateInput) return null;
  
  try {
    const dateObj = new Date(dateInput);
    if (isNaN(dateObj.getTime())) return null;
    
    // Converter para fuso horário do Brasil
    const brazilDate = new Date(dateObj.toLocaleString('en-US', {timeZone: 'America/Sao_Paulo'}));
    const year = brazilDate.getFullYear();
    const month = String(brazilDate.getMonth() + 1).padStart(2, '0');
    const day = String(brazilDate.getDate()).padStart(2, '0');
    
    return `${year}-${month}-${day}`;
  } catch (err) {
    console.error('❌ Erro ao converter data para fuso do Brasil:', err.message);
    return null;
  }
}

// Função auxiliar para atualizar estatísticas por campanha
// increment: 1 para adicionar, -1 para remover
function updateCampaignStats(campanha, campanhaId, conjunto, conjuntoId, anuncio, anuncioId, placement, siteSource, tipo, increment = 1) {
  if (!db || !campanha) return;

  const campanhaValue = campanha || 'N/A';
  const campanhaIdValue = campanhaId || null;
  const conjuntoValue = conjunto || 'N/A';
  const conjuntoIdValue = conjuntoId || null;
  const anuncioValue = anuncio || 'N/A';
  const anuncioIdValue = anuncioId || null;
  const placementValue = placement || null;
  const siteSourceValue = siteSource || null;

  // Determinar qual campo incrementar baseado no tipo
  let fieldToUpdate = 'leads';
  if (tipo === 'conversao' || tipo === 'approval') fieldToUpdate = 'conversoes';
  else if (tipo === 'trash') fieldToUpdate = 'trash';
  else if (tipo === 'cancel' || tipo === 'rejection') fieldToUpdate = 'cancel';
  else fieldToUpdate = 'leads'; // padrão é lead

  // Obter horário atual do Brasil para updated_at
  const brazilDateTime = getBrazilDateTimeSQL();
  
  // Usar INSERT OR REPLACE para criar ou atualizar
  // Se increment for negativo, decrementar (mas não deixar negativo)
  const incrementValue = increment;
  // SQLite não tem MAX() para comparação, usar CASE WHEN
  const updateExpression = increment > 0 
    ? `${fieldToUpdate} = ${fieldToUpdate} + ${incrementValue}`
    : `${fieldToUpdate} = CASE WHEN (${fieldToUpdate} + ${incrementValue}) < 0 THEN 0 ELSE (${fieldToUpdate} + ${incrementValue}) END`;
  
  const sql = `INSERT INTO campaign_stats (campanha, campanha_id, conjunto, conjunto_id, anuncio, anuncio_id, placement, site_source, ${fieldToUpdate}, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(campanha, conjunto, anuncio) 
               DO UPDATE SET ${updateExpression}, 
                             campanha_id = COALESCE(?, campanha_id),
                             conjunto_id = COALESCE(?, conjunto_id),
                             anuncio_id = COALESCE(?, anuncio_id),
                             placement = COALESCE(?, placement),
                             site_source = COALESCE(?, site_source),
                             updated_at = ?`;
  
  const initialValue = increment > 0 ? increment : 0;
  
  db.run(sql, [campanhaValue, campanhaIdValue, conjuntoValue, conjuntoIdValue, anuncioValue, anuncioIdValue, placementValue, siteSourceValue, initialValue, brazilDateTime,
               campanhaIdValue, conjuntoIdValue, anuncioIdValue, placementValue, siteSourceValue, brazilDateTime], (err) => {
    if (err) {
      console.error('❌ Erro ao atualizar estatísticas:', err.message);
    } else {
      const action = increment > 0 ? 'adicionado' : 'removido';
      console.log(`✅ Estatísticas atualizadas: ${campanhaValue} - ${tipo} (${action})`);
    }
  });
}

// Função auxiliar para processar postback
function processPostback(req, res, notificationType) {
  // Receber TODOS os parâmetros que a LeadRock enviar
  const allParams = req.query;
  
  // ============================================
  // MAPEAMENTO HIERÁRQUICO: Campanha > Conjunto > Anúncio
  // ============================================
  
  // Função auxiliar para validar se um valor é um placeholder não substituído
  function isValidValue(value) {
    if (!value || typeof value !== 'string') return false;
    // Se contém chaves {}, provavelmente é um placeholder não substituído
    if (value.includes('{') && value.includes('}')) return false;
    // Se é exatamente um placeholder conhecido
    const placeholders = ['{leadId}', '{offerId}', '{offer_id}', '{lead_id}', '{sub1}', '{sub2}', '{sub3}', '{sub4}', '{sub5}', '{sub6}', '{sub_id1}', '{sub_id2}', '{sub_id3}', '{sub_id4}', '{sub_id5}', '{sub_id6}'];
    if (placeholders.includes(value)) return false;
    return true;
  }

  // Função auxiliar para validar se um valor é um placeholder não substituído
  function isValidValue(value) {
    if (!value || typeof value !== 'string') return false;
    // Se contém chaves {}, provavelmente é um placeholder não substituído
    if (value.includes('{') && value.includes('}')) return false;
    // Se é exatamente um placeholder conhecido
    const placeholders = ['{leadId}', '{offerId}', '{offer_id}', '{lead_id}', '{sub1}', '{sub2}', '{sub3}', '{sub4}', '{sub5}', '{sub6}', '{sub_id1}', '{sub_id2}', '{sub_id3}', '{sub_id4}', '{sub_id5}', '{sub_id6}'];
    if (placeholders.includes(value)) return false;
    return true;
  }

  // Parâmetros Sub IDs (formato LeadRock/Facebook)
  // Hierarquia esperada:
  // - sub6 ou sub_id6 = Nome da Campanha (campaign.name) - NÍVEL 1
  // - sub5 ou sub_id5 = Nome do Conjunto (adset.name) - NÍVEL 2
  // - sub4 ou sub_id4 = Nome do Anúncio (ad.name) - NÍVEL 3
  const sub1_raw = allParams.sub1 || allParams.sub_id1 || allParams['sub_id'] || null;
  const sub1_value = isValidValue(sub1_raw) ? sub1_raw : null;
  
  const sub2_raw = allParams.sub2 || allParams.sub_id2 || allParams.sub_id_2 || null;
  const sub2_value = isValidValue(sub2_raw) ? sub2_raw : null;
  
  const sub3_raw = allParams.sub3 || allParams.sub_id3 || allParams.sub_id_3 || null;
  const sub3_value = isValidValue(sub3_raw) ? sub3_raw : null;
  
  const sub4_raw = allParams.sub4 || allParams.sub_id4 || allParams.sub_id_4 || null; // ad.name (Anúncio)
  const sub4_value = isValidValue(sub4_raw) ? sub4_raw : null;
  
  const sub5_raw = allParams.sub5 || allParams.sub_id5 || allParams.sub_id_5 || null; // adset.name (Conjunto)
  const sub5_value = isValidValue(sub5_raw) ? sub5_raw : null;
  
  const sub6_raw = allParams.sub6 || allParams.sub_id6 || allParams.sub_id_6 || null; // campaign.name (Campanha)
  const sub6_value = isValidValue(sub6_raw) ? sub6_raw : null;
  
  // UTM parameters (também contêm os nomes hierárquicos)
  const utm_campaign_raw = allParams.utm_campaign || null; // campaign.name (Campanha)
  const utm_campaign = isValidValue(utm_campaign_raw) ? utm_campaign_raw : null;
  
  const utm_content_raw = allParams.utm_content || null; // adset.name (Conjunto)
  const utm_content = isValidValue(utm_content_raw) ? utm_content_raw : null;
  
  const utm_term_raw = allParams.utm_term || null; // ad.name (Anúncio)
  const utm_term = isValidValue(utm_term_raw) ? utm_term_raw : null;
  
  const utm_source_raw = allParams.utm_source || null;
  const utm_source = isValidValue(utm_source_raw) ? utm_source_raw : null;
  
  const utm_medium_raw = allParams.utm_medium || null;
  const utm_medium = isValidValue(utm_medium_raw) ? utm_medium_raw : null;
  
  // Parâmetros diretos (fallback)
  const direct_campaign_raw = allParams.campaign || allParams.campaign_name || allParams.campanha || null;
  const direct_campaign = isValidValue(direct_campaign_raw) ? direct_campaign_raw : null;
  
  const direct_adset_raw = allParams.adset || allParams.adset_name || allParams.conjunto || null;
  const direct_adset = isValidValue(direct_adset_raw) ? direct_adset_raw : null;
  
  const direct_ad_raw = allParams.ad || allParams.ad_name || allParams.anuncio || null;
  const direct_ad = isValidValue(direct_ad_raw) ? direct_ad_raw : null;
  
  // Placement e Pixel
  const placement = allParams.placement || allParams.sub7 || allParams.sub_id7 || null;
  const pixel = allParams.pixel || null;
  
  // IDs (se disponíveis)
  const ad_id = allParams.ad_id || sub2_value || null;
  const adset_id = allParams.adset_id || sub3_value || null;
  const campaign_id = allParams.campaign_id || sub3_value || null;
  
  // ============================================
  // MAPEAMENTO HIERÁRQUICO FINAL
  // Prioridade: UTM > Sub IDs > Parâmetros diretos
  // Hierarquia: Campanha (Nível 1) > Conjunto (Nível 2) > Anúncio (Nível 3)
  // ============================================
  
  // CAMPANHA (Nível 1 - Hierarquia Superior)
  // Prioridade: utm_campaign > sub6 (campaign.name) > sub3 > parâmetros diretos
  const campanha = utm_campaign || sub6_value || sub3_value || direct_campaign || null;
  
  // CONJUNTO (Nível 2 - Hierarquia Média - dentro da Campanha)
  // Prioridade: utm_content > sub5 (adset.name) > parâmetros diretos
  // NÃO usar sub4 aqui para evitar confusão com anúncio
  const conjunto = utm_content || sub5_value || direct_adset || null;
  
  // ANÚNCIO (Nível 3 - Hierarquia Inferior - dentro do Conjunto)
  // Prioridade: utm_term > sub4 (ad.name) > parâmetros diretos
  const anuncio = utm_term || sub4_value || direct_ad || null;
  
  // Valores finais garantindo hierarquia
  let campanha_final = campanha;
  let conjunto_final = conjunto;
  let anuncio_final = anuncio;
  
  // Validação e correção de hierarquia
  // Se temos dados mas a hierarquia está invertida, corrigir
  if (sub4_value && sub5_value && !campanha_final) {
    // Se temos sub4 e sub5 mas não campanha, pode ser formato alternativo
    // Tentar inferir: se sub3 existe, pode ser campanha
    if (sub3_value) {
      campanha_final = sub3_value;
      conjunto_final = sub5_value;
      anuncio_final = sub4_value;
    }
  }
  
  // Garantir que se temos campanha, conjunto e anúncio, a hierarquia está correta
  // Se temos apenas um nível, manter como está
  // Se temos dois níveis, garantir ordem correta
  if (campanha_final && !conjunto_final && anuncio_final) {
    // Se temos campanha e anúncio mas não conjunto, anúncio pode estar no lugar errado
    // Manter como está, conjunto pode ser opcional
  }
  
  // Outros parâmetros
  // Separar leadId e offerId - a LeadRock envia ambos separadamente
  // LeadId: ID único do lead na LeadRock
  const lead_id_raw = allParams.leadId || 
                      allParams.lead_id || 
                      allParams.leadid || 
                      null;
  const lead_id = isValidValue(lead_id_raw) ? lead_id_raw : null;
  
  // OfferId: ID da oferta no sistema
  const offer_id_raw = allParams.offer_id || 
                       allParams.offerId || 
                       allParams.offerid || 
                       allParams.order_id || 
                       allParams.orderId || 
                       allParams.orderid ||
                       allParams.order ||
                       allParams.id ||
                       null;
  const offer_id = isValidValue(offer_id_raw) ? offer_id_raw : null;
  const status = allParams.status || allParams.state || null;
  // Priorizar 'price' que a LeadRock envia automaticamente da coluna Price
  const payout = allParams.price || allParams.payout || allParams.amount || allParams.value || allParams.revenue || null;
  
  // Função auxiliar para obter data atual no formato YYYY-MM-DD (horário Brasil) - local
  function getTodayDateLocal() {
    return getTodayDate(); // getTodayDate() retorna data baseada no horário do Brasil
  }
  
  // Processar e normalizar data do postback
  // IMPORTANTE: Se não houver data no postback, SEMPRE usar a data atual (hoje)
  let date = allParams.date || allParams.timestamp || allParams.time || null;
  
  console.log('🔍 Parâmetros de data recebidos:', {
    date: allParams.date,
    timestamp: allParams.timestamp,
    time: allParams.time,
    dateProcessed: date
  });
  
  if (date) {
    try {
      // Tentar converter para formato ISO (YYYY-MM-DD)
      const dateObj = new Date(date);
      if (!isNaN(dateObj.getTime())) {
        // Converter para fuso horário do Brasil
        const brazilDateStr = getDateInBrazilTimezone(date);
        if (brazilDateStr) {
          date = brazilDateStr;
          console.log(`📅 Data processada do postback (horário Brasil): ${date}`);
        } else {
          // Se não conseguir converter, usar data atual do Brasil
          console.log('⚠️ Data não pôde ser convertida para fuso do Brasil, usando data atual:', date);
          date = getTodayDateLocal();
        }
      } else {
        // Se não conseguir converter, usar data atual do Brasil
        console.log('⚠️ Data não pôde ser convertida, usando data atual:', date);
        date = getTodayDateLocal();
      }
    } catch (err) {
      // Se houver erro, usar data atual do Brasil
      console.log('⚠️ Erro ao processar data, usando data atual:', err.message);
      date = getTodayDateLocal();
    }
  } else {
    // Se não houver parâmetro date, SEMPRE usar a data atual baseada no horário do Brasil
    date = getTodayDateLocal();
    console.log('📅 Nenhuma data recebida no postback, usando data atual (horário Brasil):', date);
  }
  
  // Log final da data que será salva
  console.log(`✅ Data final que será salva: ${date}`);

  // Log COMPLETO de todos os parâmetros recebidos
  console.log(`\n📥 POSTBACK RECEBIDO (${notificationType.toUpperCase()}):`);
  console.log('  - Tipo:', notificationType);
  console.log('  - Timestamp:', new Date().toISOString());
  console.log('  - TODOS OS PARÂMETROS RECEBIDOS:');
  Object.keys(allParams).forEach(key => {
    console.log(`    ${key}: ${allParams[key]}`);
  });
  console.log('  - Mapeamento LeadRock/Facebook (Hierárquico):');
  console.log('    sub1/sub_id1:', sub1_value || 'N/A');
  console.log('    sub2/sub_id2:', sub2_value || 'N/A');
  console.log('    sub3/sub_id3:', sub3_value || 'N/A');
  console.log('    sub4/sub_id4 (ad.name - Anúncio):', sub4_value || 'N/A');
  console.log('    sub5/sub_id5 (adset.name - Conjunto):', sub5_value || 'N/A');
  console.log('    sub6/sub_id6 (campaign.name - Campanha):', sub6_value || 'N/A');
  console.log('    utm_campaign (Campanha):', utm_campaign || 'N/A');
  console.log('    utm_content (Conjunto):', utm_content || 'N/A');
  console.log('    utm_term (Anúncio):', utm_term || 'N/A');
  console.log('    utm_source:', utm_source || 'N/A');
  console.log('    utm_medium:', utm_medium || 'N/A');
  console.log('    placement:', placement || 'N/A');
  console.log('    pixel:', pixel || 'N/A');
  console.log('  - Hierarquia Final (Campanha > Conjunto > Anúncio):');
  console.log('    📊 Campanha (Nível 1):', campanha_final || 'N/A');
  console.log('    📁 Conjunto (Nível 2):', conjunto_final || 'N/A');
  console.log('    📄 Anúncio (Nível 3):', anuncio_final || 'N/A');
  console.log('    Lead ID:', lead_id || 'N/A');
  if (lead_id_raw && !lead_id) {
    console.log('    ⚠️ Lead ID inválido (placeholder não substituído):', lead_id_raw);
  }
  console.log('    Offer ID:', offer_id || 'N/A');
  if (offer_id_raw && !offer_id) {
    console.log('    ⚠️ Offer ID inválido (placeholder não substituído):', offer_id_raw);
  }
  console.log('    Status:', status || 'N/A');
  console.log('    Price/Payout:', payout || 'N/A');
  console.log('    Date:', date || 'N/A (será usado created_at)');
  
  // Log detalhado de parâmetros relacionados a leadId e offerId
  console.log('  - Parâmetros Lead ID recebidos:');
  const leadIdParams = ['leadId', 'lead_id', 'leadid'];
  leadIdParams.forEach(param => {
    if (allParams[param]) {
      const isValid = isValidValue(allParams[param]);
      console.log(`    ${param}: ${allParams[param]} ${isValid ? '✅' : '❌ (placeholder não substituído)'}`);
    }
  });
  
  console.log('  - Parâmetros Offer ID recebidos:');
  const offerIdParams = ['offer_id', 'offerId', 'offerid', 'order_id', 'orderId', 'orderid', 'order', 'id'];
  offerIdParams.forEach(param => {
    if (allParams[param]) {
      const isValid = isValidValue(allParams[param]);
      console.log(`    ${param}: ${allParams[param]} ${isValid ? '✅' : '❌ (placeholder não substituído)'}`);
    }
  });
  
  // Avisar se há placeholders não substituídos
  const hasInvalidParams = (lead_id_raw && !lead_id) || (offer_id_raw && !offer_id) || 
                           (sub3_value === null && sub3_raw) || (sub4_value === null && sub4_raw) || 
                           (sub5_value === null && sub5_raw) || (sub6_value === null && sub6_raw);
  if (hasInvalidParams) {
    console.log('  ⚠️ ATENÇÃO: Placeholders não substituídos detectados! Verifique a configuração das URLs na LeadRock.');
    console.log('     Os placeholders devem ser substituídos automaticamente pela LeadRock.');
    console.log('     Exemplo correto: leadId=abc123 (não {leadId})');
  }

  // Verificar se banco está disponível
  if (!db) {
    console.error('❌ Banco de dados não está disponível');
    return res.status(500).json({ success: false, error: 'Banco de dados não disponível' });
  }

  // IMPORTANTE: created_at e date sempre serão no horário de São Paulo/Brasil
  // created_at = timestamp exato de quando o lead chegou ao servidor
  // date = data para contagem/agrupamento dos leads
  const brazilDateTime = getBrazilDateTimeSQL();
  
  console.log(`🕐 Horário de chegada do lead (Brasil - será salvo em created_at): ${brazilDateTime}`);
  console.log(`📅 Data que será atribuída ao lead (horário Brasil): ${date}`);
  
  // LÓGICA CORRIGIDA:
  // 1. Se for notificação de 'lead' (primeira vez), SEMPRE criar novo, EXCETO se tiver lead_id duplicado
  //    (lead_id é único por lead, então se já existe um lead com aquele lead_id, é o mesmo lead)
  // 2. Se for atualização de status (conversao, cancel, trash), verificar se existe para atualizar
  
  console.log(`\n🔍 PROCESSANDO: Tipo='${notificationType}', lead_id='${lead_id || 'N/A'}', offer_id='${offer_id || 'N/A'}'`);
  
  if (notificationType === 'lead') {
    // Para novos leads, só verificar se já existe se tiver lead_id (que é único)
    // Se tiver apenas offer_id, SEMPRE criar novo (mesmo offer_id pode ter múltiplos leads)
    if (lead_id) {
      // Verificar se já existe lead com este lead_id
      db.get('SELECT id, notification_type, offer_id, lead_id FROM conversions WHERE lead_id = ? LIMIT 1', 
        [lead_id], (err, existingLead) => {
          if (err) {
            console.error('❌ Erro ao verificar lead_id existente:', err.message);
            insertNewLead();
            return;
          }
          
          if (existingLead) {
            // Lead com este lead_id já existe, atualizar
            console.log(`🔄 Lead com lead_id '${lead_id}' já existe (ID: ${existingLead.id}), atualizando`);
            
            // Buscar categoria do produto se houver offer_id
            const finalOfferId = offer_id || existingLead.offer_id;
            db.get('SELECT nome_conta FROM produtos WHERE offer_id = ?', [finalOfferId], (errProd, produto) => {
              const categoriaAtual = produto ? produto.nome_conta : null;
              
              const updateSql = `UPDATE conversions 
                                SET notification_type = ?, 
                                    status = ?, 
                                    payout = ?,
                                    offer_id = COALESCE(?, offer_id),
                                    categoria = COALESCE(?, categoria)
                                WHERE id = ?`;
              
              db.run(updateSql, [
                notificationType,
                status || null,
                payout ? parseFloat(payout) : null,
                offer_id || null,
                categoriaAtual || null,
                existingLead.id
              ], function(updateErr) {
                if (updateErr) {
                  console.error('❌ Erro ao atualizar lead:', updateErr.message);
                  return res.status(500).json({ success: false, error: 'Erro ao atualizar lead' });
                }
                
                console.log('✅ Lead atualizado com sucesso (ID:', existingLead.id + ')');
                res.json({ success: true, id: existingLead.id, updated: true });
              });
            });
          } else {
            // Lead_id não existe, criar novo
            console.log(`📝 Novo lead com lead_id '${lead_id}', criando registro`);
            insertNewLead();
          }
        });
    } else {
      // Sem lead_id, SEMPRE criar novo (mesmo que tenha offer_id)
      console.log('📝 Novo lead sem lead_id, criando registro direto');
      insertNewLead();
    }
  } else {
    // É atualização de status (conversao, cancel, trash)
    // IMPORTANTE: Buscar lead existente SEM usar a data atual como critério
    // A data deve ser mantida do lead original (dia 22), não da atualização (dia 23)
    // Prioridade: lead_id > offer_id > hierarquia (sem data)
    // Busca melhorada: priorizar lead_id, depois offer_id, depois hierarquia
    // IMPORTANTE: Sem restrição de data para permitir correlação mesmo se passou muito tempo
    const checkSql = `SELECT id, notification_type, offer_id, lead_id, date FROM conversions 
                      WHERE (
                        (? IS NOT NULL AND lead_id = ?)
                        OR (? IS NOT NULL AND offer_id = ?)
                        OR (
                          sub_id1 = ? 
                          AND campanha = ? 
                          AND conjunto = ? 
                          AND anuncio = ? 
                          AND notification_type = 'lead'
                          -- Sem restrição de data para permitir correlação mesmo se passou muito tempo
                        )
                      )
                      ORDER BY 
                        CASE WHEN lead_id IS NOT NULL AND lead_id = ? THEN 1 
                             WHEN offer_id IS NOT NULL AND offer_id = ? THEN 2 
                             WHEN lead_id IS NOT NULL THEN 3
                             WHEN offer_id IS NOT NULL THEN 4
                             ELSE 5 END,
                        created_at DESC 
                      LIMIT 1`;
    
    db.get(checkSql, [
      lead_id, lead_id, // Para lead_id (primeira condição)
      offer_id, offer_id, // Para offer_id (segunda condição)
      sub1_value, campanha_final, conjunto_final, anuncio_final, // hierarquia (terceira condição)
      lead_id, // Para ORDER BY (prioridade lead_id)
      offer_id  // Para ORDER BY (prioridade offer_id)
    ], (err, existingLead) => {
      if (err) {
        console.error('❌ Erro ao verificar lead existente:', err.message);
        insertNewLead();
        return;
      }
      
      if (existingLead) {
        // Lead existe, atualizar status
        console.log(`🔄 Lead existente encontrado (ID: ${existingLead.id}), atualizando status de '${existingLead.notification_type}' para '${notificationType}'`);
        console.log(`   📅 Data original do lead (será mantida): ${existingLead.date || 'N/A'}`);
        console.log(`   📅 Data da atualização (será ignorada): ${date || 'N/A'}`);
        
        // Buscar categoria do produto se houver offer_id
        const finalOfferId = offer_id || existingLead.offer_id;
        db.get('SELECT nome_conta FROM produtos WHERE offer_id = ?', [finalOfferId], (errProd, produto) => {
          const categoriaAtual = produto ? produto.nome_conta : null;
          
          // CRÍTICO: NÃO atualizar a data - manter a data original do lead
          // A data deve permanecer como estava quando o lead foi criado (dia 22)
          // Não usar a data da atualização (dia 23)
          const updateSql = `UPDATE conversions 
                            SET notification_type = ?, 
                                status = ?, 
                                payout = ?,
                                lead_id = COALESCE(?, lead_id),
                                offer_id = COALESCE(?, offer_id),
                                categoria = COALESCE(?, categoria)
                                -- date NÃO é atualizado - mantém a data original
                            WHERE id = ?`;
          
          db.run(updateSql, [
            notificationType,
            status || null,
            payout ? parseFloat(payout) : null,
            lead_id || null,
            offer_id || null,
            categoriaAtual || null,
            existingLead.id
          ], function(updateErr) {
            if (updateErr) {
              console.error('❌ Erro ao atualizar lead:', updateErr.message);
              return res.status(500).json({ success: false, error: 'Erro ao atualizar lead' });
            }
            
            console.log('✅ Lead atualizado com sucesso (ID:', existingLead.id + ')');
            console.log('   Status atualizado:', existingLead.notification_type, '→', notificationType);
            console.log('   Data mantida (original):', existingLead.date || 'N/A');
            if (categoriaAtual) {
              console.log('   Categoria associada:', categoriaAtual);
            }
            
            // Atualizar estatísticas (remover do tipo antigo, adicionar ao novo)
            // IMPORTANTE: Usar a data original do lead para atualizar as estatísticas
            if (existingLead.notification_type !== notificationType) {
              // Remover do tipo antigo na data original
              updateCampaignStats(campanha_final, campaign_id, conjunto_final, adset_id, anuncio_final, ad_id, placement, utm_source, existingLead.notification_type, -1);
              // Adicionar ao novo tipo na data original
              updateCampaignStats(campanha_final, campaign_id, conjunto_final, adset_id, anuncio_final, ad_id, placement, utm_source, notificationType, 1);
            }
            
            res.json({ success: true, id: existingLead.id, updated: true });
          });
        });
      } else {
        // Lead não existe
        // IMPORTANTE: Se não temos lead_id nem offer_id, e não encontramos um lead existente,
        // ainda podemos tentar criar um novo. Mas se temos lead_id ou offer_id, deveria ter encontrado.
        // Por segurança, vamos criar novo apenas se for realmente um novo lead
        // (se for atualização de status, a LeadRock DEVE enviar lead_id ou offer_id)
        if (!lead_id && !offer_id) {
          // Sem identificadores, pode ser um novo lead
          console.log('📝 Lead não encontrado e sem identificadores (lead_id/offer_id), criando novo registro');
          insertNewLead();
        } else {
          // Temos lead_id ou offer_id mas não encontramos o lead existente
          // Isso pode acontecer se o lead foi criado há muito tempo ou em outra campanha
          // Por segurança, vamos criar novo registro mas logar o aviso
          console.log('⚠️ AVISO: Lead não encontrado mas temos identificadores:', { lead_id, offer_id });
          console.log('   Isso pode indicar um problema de correlação. Criando novo registro como fallback.');
          insertNewLead();
        }
      }
    });
  }
  
  // Função para inserir novo lead
  function insertNewLead() {
    // Buscar categoria do produto se houver offer_id
    db.get('SELECT nome_conta FROM produtos WHERE offer_id = ?', [offer_id], (errProd, produto) => {
      const categoriaProduto = produto ? produto.nome_conta : null;
      
      const sql = `INSERT INTO conversions (sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, sub_id6, sub_id7, sub_id8, campanha, conjunto, anuncio, offer_id, lead_id, categoria, status, payout, date, notification_type, utm_source, utm_medium, created_at) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
      
      db.run(sql, [
        sub1_value,        // sub_id1 = valor fixo (ex: Jeff-10x5962)
        ad_id,             // sub_id2 = ad.id (se disponível)
        campaign_id,       // sub_id3 = campaign.id (se disponível)
        anuncio_final,     // sub_id4 = ad.name (Anúncio - Nível 3)
        conjunto_final,    // sub_id5 = adset.name (Conjunto - Nível 2)
        campanha_final,    // sub_id6 = campaign.name (Campanha - Nível 1)
        placement,         // sub_id7 = placement
        pixel,             // sub_id8 = pixel ID
        campanha_final,    // campanha = nome da campanha (hierarquia nível 1)
        conjunto_final,    // conjunto = nome do conjunto (hierarquia nível 2)
        anuncio_final,     // anuncio = nome do anúncio (hierarquia nível 3)
        offer_id || null,  // offer_id = ID da oferta
        lead_id || null,   // lead_id = ID único do lead na LeadRock
        categoriaProduto || null, // categoria = nome da conta do produto
        status || null, 
        payout ? parseFloat(payout) : null, 
        date || null, 
        notificationType,
        utm_source || null,
        utm_medium || null,
        brazilDateTime     // created_at = horário de São Paulo/Brasil (timestamp de chegada)
      ], function(err) {
    if (err) {
      console.error('❌ Erro ao salvar no banco:', err.message);
      return res.status(500).json({ success: false, error: 'Erro ao salvar dados' });
    }
    
    console.log('✅ Dados salvos com sucesso (ID:', this.lastID + ')');
    console.log('   Hierarquia salva:');
    console.log('     Campanha:', campanha_final || 'N/A');
    console.log('     Conjunto:', conjunto_final || 'N/A');
    console.log('     Anúncio:', anuncio_final || 'N/A');
    
    // Atualizar estatísticas por campanha (usando hierarquia: campanha > conjunto > anuncio)
    updateCampaignStats(campanha_final, campaign_id, conjunto_final, adset_id, anuncio_final, ad_id, placement, utm_source, notificationType);
    
      res.json({ success: true, id: this.lastID });
    });
    });
  }
}

// Rota genérica para postback (mantida para compatibilidade)
app.get('/postback', (req, res) => {
  processPostback(req, res, 'lead');
});

// Rota para notificação de Lead (objetivo alcançado)
app.get('/postback/lead', (req, res) => {
  processPostback(req, res, 'lead');
});

// Rota para notificação de Conversão (aprovação)
app.get('/postback/conversao', (req, res) => {
  processPostback(req, res, 'conversao');
});

// Rota para notificação de Trash
app.get('/postback/trash', (req, res) => {
  processPostback(req, res, 'trash');
});

// Rota para notificação de Cancel (rejeição)
app.get('/postback/cancel', (req, res) => {
  processPostback(req, res, 'cancel');
});

// Função auxiliar para normalizar data para formato YYYY-MM-DD
function normalizeDate(dateString) {
  if (!dateString) return null;
  try {
    const date = new Date(dateString);
    if (!isNaN(date.getTime())) {
      return date.toISOString().split('T')[0];
    }
  } catch (err) {
    // Ignorar erro
  }
  return dateString;
}

// Rota API para buscar conversões (com suporte a filtro por data)
app.get('/api/conversions', (req, res) => {
  // Verificar se banco está disponível
  if (!db) {
    console.error('❌ Banco de dados não está disponível');
    return res.status(500).json({ error: 'Banco de dados não disponível' });
  }

  // Verificar se há filtro de data, produto ou conta
  const dateFilter = req.query.date || null;
  const startDate = req.query.startDate || null;
  const endDate = req.query.endDate || null;
  const offerIdFilter = req.query.offerId || null;
  const categoriaFilter = req.query.categoria || null;

  let sql = `SELECT * FROM conversions`;
  const params = [];

  // Construir WHERE clause baseado nos filtros
  const conditions = [];
  
  // Filtro por Offer ID (produto) - prioridade
  if (offerIdFilter) {
    conditions.push(`offer_id = ?`);
    params.push(offerIdFilter);
  }
  
  // Filtro por Conta (categoria - nome_conta do produto)
  if (categoriaFilter) {
    conditions.push(`categoria = ?`);
    params.push(categoriaFilter);
  }
  
  if (dateFilter) {
    // Filtro por data específica (YYYY-MM-DD)
    const normalizedDate = normalizeDate(dateFilter);
    if (normalizedDate) {
      conditions.push(`(date(date) = date(?) OR (date IS NULL AND date(created_at) = date(?)))`);
      params.push(normalizedDate, normalizedDate);
    }
  } else if (startDate || endDate) {
    // Filtro por intervalo de datas
    if (startDate) {
      const normalizedStart = normalizeDate(startDate);
      if (normalizedStart) {
        conditions.push(`(date(date) >= date(?) OR (date IS NULL AND date(created_at) >= date(?)))`);
        params.push(normalizedStart, normalizedStart);
      }
    }
    if (endDate) {
      const normalizedEnd = normalizeDate(endDate);
      if (normalizedEnd) {
        conditions.push(`(date(date) <= date(?) OR (date IS NULL AND date(created_at) <= date(?)))`);
        params.push(normalizedEnd, normalizedEnd);
      }
    }
  }

  if (conditions.length > 0) {
    sql += ` WHERE ${conditions.join(' AND ')}`;
  }

  sql += ` ORDER BY COALESCE(date, created_at) DESC, created_at DESC`;
  
  db.all(sql, params, (err, rows) => {
    if (err) {
      console.error('Erro ao buscar conversões:', err.message);
      return res.status(500).json({ error: 'Erro ao buscar dados' });
    }
    
    // Preencher campos vazios com valores dos sub_ids (para compatibilidade com dados antigos)
    // Respeitando hierarquia: Campanha (sub_id6) > Conjunto (sub_id5) > Anúncio (sub_id4)
    const rowsWithDefaults = rows.map(row => {
      // Se campanha está vazia, usar sub_id6 (campaign.name) ou sub_id3 como fallback
      if (!row.campanha) {
        row.campanha = row.sub_id6 || row.sub_id3 || null;
      }
      // Se conjunto está vazio, usar sub_id5 (adset.name)
      if (!row.conjunto) {
        row.conjunto = row.sub_id5 || null;
      }
      // Se anuncio está vazio, usar sub_id4 (ad.name)
      if (!row.anuncio) {
        row.anuncio = row.sub_id4 || null;
      }
      // Normalizar data para exibição
      if (row.date) {
        row.date = normalizeDate(row.date);
      }
      return row;
    });
    
    res.json(rowsWithDefaults);
  });
});

// Rota API para buscar extrato completo de postbacks
app.get('/api/extrato', (req, res) => {
  if (!db) {
    console.error('❌ Banco de dados não está disponível');
    return res.status(500).json({ error: 'Banco de dados não disponível' });
  }

  // Verificar se há filtro de data ou produto
  const dateFilter = req.query.date || null;
  const startDate = req.query.startDate || null;
  const endDate = req.query.endDate || null;
  const offerIdFilter = req.query.offerId || null;

  let sql = `SELECT * FROM conversions`;
  const params = [];

  // Construir WHERE clause baseado nos filtros
  const conditions = [];
  
  // Filtro por Offer ID (produto) - prioridade
  if (offerIdFilter) {
    conditions.push(`offer_id = ?`);
    params.push(offerIdFilter);
  }
  
  if (dateFilter) {
    // Verificar se é "today" para usar data atual do Brasil
    let dateToUse = null;
    if (dateFilter.toLowerCase() === 'today' || dateFilter.toLowerCase() === 'hoje') {
      dateToUse = getTodayDate();
      console.log(`📅 [EXTRATO] Filtro "today" detectado, usando data atual (Brasil): ${dateToUse}`);
    } else {
      // Filtro por data específica (YYYY-MM-DD)
      dateToUse = normalizeDate(dateFilter);
    }
    
    if (dateToUse) {
      conditions.push(`(date(date) = date(?) OR (date IS NULL AND date(created_at) = date(?)))`);
      params.push(dateToUse, dateToUse);
    }
  } else if (startDate || endDate) {
    // Filtro por intervalo de datas
    if (startDate) {
      const normalizedStart = normalizeDate(startDate);
      if (normalizedStart) {
        conditions.push(`(date(date) >= date(?) OR (date IS NULL AND date(created_at) >= date(?)))`);
        params.push(normalizedStart, normalizedStart);
      }
    }
    if (endDate) {
      const normalizedEnd = normalizeDate(endDate);
      if (normalizedEnd) {
        conditions.push(`(date(date) <= date(?) OR (date IS NULL AND date(created_at) <= date(?)))`);
        params.push(normalizedEnd, normalizedEnd);
      }
    }
  }

  if (conditions.length > 0) {
    sql += ` WHERE ${conditions.join(' AND ')}`;
  }

  sql += ` ORDER BY created_at DESC, id DESC`;
  
  db.all(sql, params, (err, rows) => {
    if (err) {
      console.error('❌ Erro ao buscar extrato:', err.message);
      return res.status(500).json({ error: 'Erro ao buscar dados', details: err.message });
    }
    
    // Preencher campos vazios com valores dos sub_ids (para compatibilidade)
    const rowsWithDefaults = rows.map(row => {
      // Se campanha está vazia, usar sub_id6 (campaign.name) ou sub_id3 como fallback
      if (!row.campanha) {
        row.campanha = row.sub_id6 || row.sub_id3 || null;
      }
      // Se conjunto está vazio, usar sub_id5 (adset.name)
      if (!row.conjunto) {
        row.conjunto = row.sub_id5 || null;
      }
      // Se anuncio está vazio, usar sub_id4 (ad.name)
      if (!row.anuncio) {
        row.anuncio = row.sub_id4 || null;
      }
      // Normalizar data para exibição
      if (row.date) {
        row.date = normalizeDate(row.date);
      }
      return row;
    });
    
    console.log(`📋 Extrato: ${rowsWithDefaults.length} registros encontrados`);
    res.json(rowsWithDefaults);
  });
});

// Rota API para buscar hierarquia (Campanhas > Conjuntos > Anúncios)
app.get('/api/hierarchy', (req, res) => {
  if (!db) {
    console.error('❌ Banco de dados não está disponível');
    return res.status(500).json({ error: 'Banco de dados não disponível' });
  }

  // Verificar se há filtro de data, produto ou conta
  const dateFilter = req.query.date || null;
  const offerIdFilter = req.query.offerId || null;
  const categoriaFilter = req.query.categoria || null;
  let sql = '';
  let params = [];
  
  // Se não houver filtro de data, usar data de hoje (Brasil)
  // IMPORTANTE: Filtrar pela data do POSTBACK (campo date ou created_at), NÃO pela data no nome da campanha
  const targetDate = dateFilter ? normalizeDate(dateFilter) : getTodayDate();
  
  console.log(`🔍 [HIERARCHY] Data filtro recebida: ${dateFilter || 'nenhuma'}, Data normalizada: ${targetDate}, Data de hoje: ${getTodayDate()}`);

  // Debug: Verificar se o lead específico está no banco
  if (targetDate) {
    db.all(`SELECT id, campanha, sub_id6, sub_id3, date, created_at, notification_type, lead_id FROM conversions WHERE (campanha LIKE '%Camp24%' OR sub_id6 LIKE '%Camp24%' OR sub_id3 LIKE '%Camp24%' OR campanha LIKE '%121CBO%' OR sub_id6 LIKE '%121CBO%' OR sub_id3 LIKE '%121CBO%') ORDER BY created_at DESC LIMIT 10`, 
      [], (errDebug, debugData) => {
        if (!errDebug && debugData.length > 0) {
          console.log(`🔍 [DEBUG] Encontrados ${debugData.length} registros Camp24 no banco (todas as datas):`);
          debugData.forEach(d => {
            const dateStr = d.date ? d.date.substring(0, 10) : '';
            const createdStr = d.created_at ? d.created_at.substring(0, 10) : '';
            const dateMatch = dateStr === targetDate;
            const createdMatch = createdStr === targetDate;
            const uniqueId = d.lead_id || ('unique_' + d.id);
            console.log(`   - ID: ${d.id}, Campanha: ${d.campanha || d.sub_id6 || d.sub_id3}, Date: ${d.date}, Created: ${d.created_at}, Unique ID: ${uniqueId}`);
            console.log(`     Data match? substr(date, 1, 10) = '${targetDate}'? ${dateMatch} (date: '${dateStr}')`);
            console.log(`     Created match? substr(created_at, 1, 10) = '${targetDate}'? ${createdMatch} (created: '${createdStr}')`);
            console.log(`     Será incluído em leads_do_dia? ${dateMatch || createdMatch}`);
          });
        } else {
          console.log(`⚠️ [DEBUG] Nenhum registro Camp24 encontrado no banco`);
        }
      });
  }

  // Sempre filtrar por data: usar data selecionada ou hoje se não houver seleção
  // Mostrar apenas campanhas que tiveram LEADS chegando naquela data (baseado na data do postback, não no nome)
  if (targetDate) {
    // Primeiro identificar leads únicos que chegaram na data selecionada
    // Usar a data do POSTBACK (date ou created_at), não a data no nome da campanha
    sql = `
      WITH leads_originais AS (
        -- Pegar o registro mais antigo de cada lead (quando chegou pela primeira vez)
        SELECT 
          c.*,
          COALESCE(
            c.lead_id,
            'unique_' || CAST(c.id AS TEXT)
          ) as unique_id,
          COALESCE(c.date, substr(c.created_at, 1, 10)) as data_original
        FROM conversions c
        WHERE c.id = (
          SELECT c2.id
          FROM conversions c2
          WHERE COALESCE(
            c2.lead_id,
            'unique_' || CAST(c2.id AS TEXT)
          ) = COALESCE(
            c.lead_id,
            'unique_' || CAST(c.id AS TEXT)
          )
          ORDER BY c2.created_at ASC
          LIMIT 1
        )
      ),
      leads_do_dia AS (
        -- Filtrar apenas leads que chegaram na data selecionada (data original)
        SELECT DISTINCT
          unique_id
        FROM leads_originais
        WHERE (
          (date IS NOT NULL AND substr(date, 1, 10) = ?)
          OR (date IS NULL AND substr(created_at, 1, 10) = ?)
        )
        ${offerIdFilter ? ' AND offer_id = ?' : ''}
        ${categoriaFilter ? ' AND categoria = ?' : ''}
      ),
      todas_conversoes AS (
        SELECT 
          c.*,
          COALESCE(
            c.lead_id,  -- Prioridade 1: lead_id é único por lead
            'unique_' || CAST(c.id AS TEXT)  -- Prioridade 2: cada registro sem lead_id é único (cada lead = 1 registro)
          ) as unique_id,
          CASE 
            WHEN c.campanha IS NOT NULL AND TRIM(c.campanha) != '' AND LOWER(TRIM(c.campanha)) != 'n/a' AND LOWER(TRIM(c.campanha)) != 'untracked' THEN TRIM(c.campanha)
            WHEN c.sub_id6 IS NOT NULL AND TRIM(c.sub_id6) != '' AND LOWER(TRIM(c.sub_id6)) != 'n/a' AND LOWER(TRIM(c.sub_id6)) != 'untracked' THEN TRIM(c.sub_id6)
            WHEN c.sub_id3 IS NOT NULL AND TRIM(c.sub_id3) != '' AND LOWER(TRIM(c.sub_id3)) != 'n/a' AND LOWER(TRIM(c.sub_id3)) != 'untracked' THEN TRIM(c.sub_id3)
            ELSE 'untracked'
          END as campanha_norm,
          -- Debug: manter campos originais para diagnóstico
          c.campanha as campanha_original,
          c.sub_id6 as sub_id6_original,
          c.sub_id3 as sub_id3_original,
          CASE 
            WHEN c.conjunto IS NOT NULL AND TRIM(c.conjunto) != '' AND LOWER(TRIM(c.conjunto)) != 'n/a' THEN TRIM(c.conjunto)
            WHEN c.sub_id5 IS NOT NULL AND TRIM(c.sub_id5) != '' AND LOWER(TRIM(c.sub_id5)) != 'n/a' THEN TRIM(c.sub_id5)
            ELSE 'untracked'
          END as conjunto_norm,
          CASE 
            WHEN c.anuncio IS NOT NULL AND TRIM(c.anuncio) != '' AND LOWER(TRIM(c.anuncio)) != 'n/a' THEN TRIM(c.anuncio)
            WHEN c.sub_id4 IS NOT NULL AND TRIM(c.sub_id4) != '' AND LOWER(TRIM(c.sub_id4)) != 'n/a' THEN TRIM(c.sub_id4)
            ELSE 'untracked'
          END as anuncio_norm
        FROM conversions c
        WHERE 1=1
        ${offerIdFilter ? ' AND c.offer_id = ?' : ''}
      ),
      conversoes_filtradas AS (
        SELECT tc.*
        FROM todas_conversoes tc
        INNER JOIN leads_do_dia ldd ON TRIM(COALESCE(ldd.unique_id, '')) = TRIM(COALESCE(tc.unique_id, ''))
      ),
      latest_conversoes AS (
        SELECT cf.*
        FROM conversoes_filtradas cf
        WHERE cf.id = (
          SELECT cf2.id
          FROM conversoes_filtradas cf2
          WHERE cf2.unique_id = cf.unique_id
          AND cf2.campanha_norm = cf.campanha_norm
          ORDER BY cf2.created_at DESC
          LIMIT 1
        )
      )
      SELECT 
        campanha_norm as campanha,
        conjunto_norm as conjunto,
        anuncio_norm as anuncio,
        COUNT(DISTINCT unique_id) as total,
        -- Contar leads: todos os leads únicos que chegaram (independente do status final)
        COUNT(DISTINCT unique_id) as leads,
        -- Contar conversões: apenas os que estão com status 'conversao' ou 'approval'
        SUM(CASE WHEN notification_type = 'conversao' OR notification_type = 'approval' THEN 1 ELSE 0 END) as conversoes,
        -- Contar cancelados: apenas os que estão com status 'cancel' ou 'rejection'
        SUM(CASE WHEN notification_type = 'cancel' OR notification_type = 'rejection' THEN 1 ELSE 0 END) as cancelados,
        -- Contar trash: apenas os que estão com status 'trash'
        SUM(CASE WHEN notification_type = 'trash' THEN 1 ELSE 0 END) as trash,
        SUM(CASE WHEN notification_type = 'conversao' OR notification_type = 'approval' THEN COALESCE(payout, 0) ELSE 0 END) as total_payout
      FROM latest_conversoes
      GROUP BY campanha_norm, conjunto_norm, anuncio_norm
      ORDER BY campanha_norm, conjunto_norm, anuncio_norm
    `;
      // Parâmetros para leads_do_dia (filtro por data original)
      params.push(targetDate, targetDate);
    if (offerIdFilter) {
      params.push(offerIdFilter); // Para leads_do_dia
    }
    if (categoriaFilter) {
      params.push(categoriaFilter); // Para leads_do_dia
    }
    // Parâmetros para todas_conversoes (filtro por produto/conta, sem data)
    if (offerIdFilter) {
      params.push(offerIdFilter); // Para todas_conversoes
    }
  } else {
    // Se não conseguir determinar a data, retornar vazio
    sql = `
      SELECT 
        'untracked' as campanha,
        'untracked' as conjunto,
        'untracked' as anuncio,
        0 as total,
        0 as leads,
        0 as conversoes,
        0 as cancelados,
        0 as trash,
        0 as total_payout
      FROM conversions
      WHERE 1=0
    `;
  }
  
  console.log(`🔍 Buscando hierarquia - Data selecionada: ${dateFilter || 'nenhuma'}, Data usada: ${targetDate || 'nenhuma'}, OfferId: ${offerIdFilter || 'nenhum'}`);
  console.log(`📝 SQL: ${sql.substring(0, 200)}...`);
  console.log(`📝 Params:`, params);
  
  // Query de diagnóstico ANTES da query principal
  if (targetDate) {
    db.get(`SELECT COUNT(*) as total, COUNT(DISTINCT COALESCE(lead_id, 'unique_' || CAST(id AS TEXT))) as unique_leads FROM conversions WHERE (substr(date, 1, 10) = ? OR (date IS NULL AND substr(created_at, 1, 10) = ?))`, 
      [targetDate, targetDate], (errDiag, diag) => {
        if (!errDiag) {
          console.log(`🔍 Diagnóstico: ${diag.total} registros totais, ${diag.unique_leads} leads únicos na data ${targetDate}`);
        }
      });
    
    // Verificar se há leads da campanha específica (Camp24)
    db.all(`SELECT id, campanha, sub_id6, sub_id3, date, created_at, notification_type, lead_id, COALESCE(lead_id, 'unique_' || CAST(id AS TEXT)) as unique_id FROM conversions WHERE (substr(date, 1, 10) = ? OR (date IS NULL AND substr(created_at, 1, 10) = ?)) AND (campanha LIKE '%Camp24%' OR sub_id6 LIKE '%Camp24%' OR sub_id3 LIKE '%Camp24%' OR campanha LIKE '%121CBO%' OR sub_id6 LIKE '%121CBO%' OR sub_id3 LIKE '%121CBO%') LIMIT 10`, 
      [targetDate, targetDate], (errCamp, campData) => {
        if (!errCamp && campData.length > 0) {
          console.log(`🔍 Diagnóstico Camp24: ${campData.length} registros encontrados para Camp24 na data ${targetDate}`);
          campData.forEach(lead => {
            console.log(`   - ID: ${lead.id}, Campanha: ${lead.campanha || lead.sub_id6 || lead.sub_id3}, Unique ID: ${lead.unique_id}, Date: ${lead.date}, Created: ${lead.created_at}, Type: ${lead.notification_type}`);
          });
        } else {
          console.log(`⚠️ Nenhum registro encontrado para Camp24 na data ${targetDate}`);
        }
      });
  }
  
  db.all(sql, params, (err, rows) => {
    if (err) {
      console.error('❌ Erro ao buscar hierarquia:', err.message);
      console.error('SQL completo:', sql);
      console.error('Params:', params);
      return res.status(500).json({ error: 'Erro ao buscar dados' });
    }
    
    console.log(`📊 Total de registros encontrados para hierarquia: ${rows.length}`);
    if (rows.length > 0) {
      console.log('📊 Primeiros registros:', rows.slice(0, 3));
      // Verificar se Camp24 está nos resultados - verificar também por "Camp24" e "121CBO" em qualquer parte do nome
      const camp24Found = rows.find(r => {
        const campanha = (r.campanha || '').toLowerCase();
        return campanha.includes('camp24') || campanha.includes('121cbo') || campanha.includes('camp24_gluc_mx_121cbo');
      });
      if (camp24Found) {
        console.log(`✅ Camp24 encontrada na hierarquia:`, camp24Found);
      } else {
        console.log(`⚠️ Camp24 NÃO encontrada na hierarquia. Verificando dados brutos...`);
        console.log(`📋 Todas as campanhas encontradas:`, rows.map(r => r.campanha).filter(c => c).slice(0, 20));
        // Verificar dados brutos e unique_ids
        if (targetDate) {
          db.all(`SELECT id, campanha, sub_id6, sub_id3, date, created_at, notification_type, lead_id, COALESCE(lead_id, 'unique_' || CAST(id AS TEXT)) as unique_id FROM conversions WHERE (substr(date, 1, 10) = ? OR (date IS NULL AND substr(created_at, 1, 10) = ?)) AND (campanha LIKE '%Camp24%' OR sub_id6 LIKE '%Camp24%' OR sub_id3 LIKE '%Camp24%' OR campanha LIKE '%121CBO%' OR sub_id6 LIKE '%121CBO%') LIMIT 5`, 
            [targetDate, targetDate], (errRaw, rawData) => {
              if (!errRaw && rawData.length > 0) {
                console.log(`🔍 Dados brutos encontrados para Camp24:`, rawData);
                // Verificar se esses unique_ids estão em leads_do_dia
                const uniqueIds = rawData.map(r => r.unique_id);
                db.all(`SELECT DISTINCT COALESCE(lead_id, 'unique_' || CAST(id AS TEXT)) as unique_id FROM conversions WHERE (substr(date, 1, 10) = ? OR (date IS NULL AND substr(created_at, 1, 10) = ?))`, 
                  [targetDate, targetDate], (errLeads, leadsDoDia) => {
                    if (!errLeads) {
                      const leadsUniqueIds = leadsDoDia.map(l => l.unique_id);
                      console.log(`🔍 Unique IDs em leads_do_dia: ${leadsUniqueIds.length} total`);
                      uniqueIds.forEach(uid => {
                        if (leadsUniqueIds.includes(uid)) {
                          console.log(`   ✅ Unique ID ${uid} está em leads_do_dia`);
                        } else {
                          console.log(`   ❌ Unique ID ${uid} NÃO está em leads_do_dia`);
                        }
                      });
                    }
                  });
              } else {
                console.log(`⚠️ Nenhum dado bruto encontrado para Camp24 na data ${targetDate}`);
              }
            });
        }
      }
    } else {
      console.log('⚠️ Nenhuma campanha encontrada. Verificando se há leads na data...');
      // Query de diagnóstico
        if (targetDate) {
          db.get(`SELECT COUNT(*) as total FROM conversions WHERE (substr(date, 1, 10) = ? OR (date IS NULL AND substr(created_at, 1, 10) = ?))`,
            [targetDate, targetDate], (err2, diag) => {
            if (!err2) {
              console.log(`🔍 Diagnóstico: ${diag.total} registros totais encontrados na data ${targetDate}`);
            }
          });
      }
    }
    
    // Organizar em estrutura hierárquica
    // Garantir que mesmo com "N/A", os dados sejam organizados
    const hierarchy = {};
    
    rows.forEach(row => {
      // Normalizar valores (evitar "N/A" quando possível, usar "untracked" quando não houver parâmetros)
      const campanha = (row.campanha && row.campanha !== 'N/A' && row.campanha !== '' && row.campanha !== 'sem-trackeamento') ? row.campanha : 'untracked';
      const conjunto = (row.conjunto && row.conjunto !== 'N/A' && row.conjunto !== '' && row.conjunto !== 'sem-trackeamento') ? row.conjunto : 'untracked';
      const anuncio = (row.anuncio && row.anuncio !== 'N/A' && row.anuncio !== '' && row.anuncio !== 'sem-trackeamento') ? row.anuncio : 'untracked';
      
      if (!hierarchy[campanha]) {
        hierarchy[campanha] = {
          nome: campanha,
          total: 0,
          leads: 0,
          conversoes: 0,
          cancelados: 0,
          trash: 0,
          total_payout: 0,
          conjuntos: {}
        };
      }
      
      if (!hierarchy[campanha].conjuntos[conjunto]) {
        hierarchy[campanha].conjuntos[conjunto] = {
          nome: conjunto,
          total: 0,
          leads: 0,
          conversoes: 0,
          cancelados: 0,
          trash: 0,
          total_payout: 0,
          anuncios: {}
        };
      }
      
      if (!hierarchy[campanha].conjuntos[conjunto].anuncios[anuncio]) {
        hierarchy[campanha].conjuntos[conjunto].anuncios[anuncio] = {
          nome: anuncio,
          total: 0,
          leads: 0,
          conversoes: 0,
          cancelados: 0,
          trash: 0,
          total_payout: 0
        };
      }
      
      // Adicionar valores do anúncio
      hierarchy[campanha].conjuntos[conjunto].anuncios[anuncio].total += row.total;
      hierarchy[campanha].conjuntos[conjunto].anuncios[anuncio].leads += row.leads;
      hierarchy[campanha].conjuntos[conjunto].anuncios[anuncio].conversoes += row.conversoes;
      hierarchy[campanha].conjuntos[conjunto].anuncios[anuncio].cancelados += row.cancelados;
      hierarchy[campanha].conjuntos[conjunto].anuncios[anuncio].trash += row.trash;
      hierarchy[campanha].conjuntos[conjunto].anuncios[anuncio].total_payout += row.total_payout;
      
      // Atualizar totais do conjunto
      hierarchy[campanha].conjuntos[conjunto].total += row.total;
      hierarchy[campanha].conjuntos[conjunto].leads += row.leads;
      hierarchy[campanha].conjuntos[conjunto].conversoes += row.conversoes;
      hierarchy[campanha].conjuntos[conjunto].cancelados += row.cancelados;
      hierarchy[campanha].conjuntos[conjunto].trash += row.trash;
      hierarchy[campanha].conjuntos[conjunto].total_payout += row.total_payout;
      
      // Atualizar totais da campanha
      hierarchy[campanha].total += row.total;
      hierarchy[campanha].leads += row.leads;
      hierarchy[campanha].conversoes += row.conversoes;
      hierarchy[campanha].cancelados += row.cancelados;
      hierarchy[campanha].trash += row.trash;
      hierarchy[campanha].total_payout += row.total_payout;
    });
    
    // Converter para array
    const result = Object.values(hierarchy).map(campanha => ({
      ...campanha,
      conjuntos: Object.values(campanha.conjuntos).map(conjunto => ({
        ...conjunto,
        anuncios: Object.values(conjunto.anuncios)
      }))
    }));
    
    console.log(`✅ Hierarquia organizada: ${result.length} campanha(s)`);
    result.forEach(c => {
      console.log(`   - ${c.nome}: ${c.conjuntos.length} conjunto(s), ${c.total} total`);
    });
    
    res.json(result);
  });
});

// Rota API para buscar estatísticas de leads por tipo
app.get('/api/stats', (req, res) => {
  if (!db) {
    console.error('❌ Banco de dados não está disponível');
    return res.status(500).json({ error: 'Banco de dados não disponível' });
  }

  // Verificar se há filtro de data, produto ou conta
  const dateFilter = req.query.date || null;
  const startDate = req.query.startDate || null;
  const endDate = req.query.endDate || null;
  const offerIdFilter = req.query.offerId || null;
  const categoriaFilter = req.query.categoria || null;

  let whereClause = '';
  const params = [];
  const conditions = [];

  // Filtro por Offer ID (produto)
  if (offerIdFilter) {
    conditions.push(`c.offer_id = ?`);
    params.push(offerIdFilter);
  }
  
  // Filtro por Conta (categoria - nome_conta do produto)
  if (categoriaFilter) {
    conditions.push(`c.categoria = ?`);
    params.push(categoriaFilter);
  }

  // Filtros de data
  if (dateFilter) {
    const normalizedDate = normalizeDate(dateFilter);
    if (normalizedDate) {
      conditions.push(`(date(c.date) = date(?) OR (c.date IS NULL AND date(c.created_at) = date(?)))`);
      params.push(normalizedDate, normalizedDate);
    }
  } else if (startDate || endDate) {
    if (startDate) {
      const normalizedStart = normalizeDate(startDate);
      if (normalizedStart) {
        conditions.push(`(date(c.date) >= date(?) OR (c.date IS NULL AND date(c.created_at) >= date(?)))`);
        params.push(normalizedStart, normalizedStart);
      }
    }
    if (endDate) {
      const normalizedEnd = normalizeDate(endDate);
      if (normalizedEnd) {
        conditions.push(`(date(c.date) <= date(?) OR (c.date IS NULL AND date(c.created_at) <= date(?)))`);
        params.push(normalizedEnd, normalizedEnd);
      }
    }
  }

  // IMPORTANTE: Contar apenas leads que chegaram na data selecionada (data original)
  // A data do lead é quando ele chegou pela primeira vez, não quando foi atualizado
  // Vamos pegar o registro mais antigo de cada lead para verificar a data original
  const allParams = [];
  
  // Construir condições de data para filtrar pela data original
  let dateCondition = '';
  if (dateFilter) {
    const normalizedDate = normalizeDate(dateFilter);
    if (normalizedDate) {
      dateCondition = `AND (original.date IS NOT NULL AND substr(original.date, 1, 10) = ? OR (original.date IS NULL AND substr(original.created_at, 1, 10) = ?))`;
      allParams.push(normalizedDate, normalizedDate);
    }
  } else if (startDate || endDate) {
    const dateConditions = [];
    if (startDate) {
      const normalizedStart = normalizeDate(startDate);
      if (normalizedStart) {
        dateConditions.push(`(original.date IS NOT NULL AND substr(original.date, 1, 10) >= ? OR (original.date IS NULL AND substr(original.created_at, 1, 10) >= ?))`);
        allParams.push(normalizedStart, normalizedStart);
      }
    }
    if (endDate) {
      const normalizedEnd = normalizeDate(endDate);
      if (normalizedEnd) {
        dateConditions.push(`(original.date IS NOT NULL AND substr(original.date, 1, 10) <= ? OR (original.date IS NULL AND substr(original.created_at, 1, 10) <= ?))`);
        allParams.push(normalizedEnd, normalizedEnd);
      }
    }
    if (dateConditions.length > 0) {
      dateCondition = `AND ${dateConditions.join(' AND ')}`;
    }
  }
  
  // Adicionar filtros de produto e conta
  if (offerIdFilter) {
    allParams.push(offerIdFilter);
  }
  if (categoriaFilter) {
    allParams.push(categoriaFilter);
  }
  
  // Query corrigida: usar data original do lead (quando chegou), não data da atualização
  const sql = `
    SELECT 
      COUNT(DISTINCT latest.unique_id) as total_leads,
      SUM(CASE WHEN latest.notification_type = 'lead' THEN 1 ELSE 0 END) as leads,
      SUM(CASE WHEN latest.notification_type = 'conversao' OR latest.notification_type = 'approval' THEN 1 ELSE 0 END) as leads_confirmados,
      SUM(CASE WHEN latest.notification_type = 'cancel' OR latest.notification_type = 'rejection' THEN 1 ELSE 0 END) as leads_cancelados,
      SUM(CASE WHEN latest.notification_type = 'trash' THEN 1 ELSE 0 END) as leads_trash,
      SUM(CASE WHEN latest.notification_type = 'conversao' OR latest.notification_type = 'approval' THEN COALESCE(latest.payout, 0) ELSE 0 END) as total_payout
    FROM (
      -- Pegar o registro mais recente de cada lead (com status atualizado)
      SELECT 
        c.*,
        COALESCE(
          c.lead_id,
          'unique_' || CAST(c.id AS TEXT)
        ) as unique_id
      FROM conversions c
      WHERE c.id = (
        SELECT c2.id
        FROM conversions c2
        WHERE COALESCE(
          c2.lead_id,
          'unique_' || CAST(c2.id AS TEXT)
        ) = COALESCE(
          c.lead_id,
          'unique_' || CAST(c.id AS TEXT)
        )
        ORDER BY c2.created_at DESC
        LIMIT 1
      )
    ) latest
    INNER JOIN (
      -- Pegar o registro mais antigo de cada lead (quando chegou pela primeira vez)
      SELECT 
        c.*,
        COALESCE(
          c.lead_id,
          'unique_' || CAST(c.id AS TEXT)
        ) as unique_id
      FROM conversions c
      WHERE c.id = (
        SELECT c2.id
        FROM conversions c2
        WHERE COALESCE(
          c2.lead_id,
          'unique_' || CAST(c2.id AS TEXT)
        ) = COALESCE(
          c.lead_id,
          'unique_' || CAST(c.id AS TEXT)
        )
        ORDER BY c2.created_at ASC
        LIMIT 1
      )
    ) original ON COALESCE(
      latest.lead_id,
      'unique_' || CAST(latest.id AS TEXT)
    ) = COALESCE(
      original.lead_id,
      'unique_' || CAST(original.id AS TEXT)
    )
    WHERE 1=1
    ${dateCondition}
    ${offerIdFilter ? 'AND latest.offer_id = ?' : ''}
    ${categoriaFilter ? 'AND latest.categoria = ?' : ''}
  `;
  
  db.get(sql, allParams, (err, row) => {
    if (err) {
      console.error('❌ Erro ao buscar estatísticas:', err.message);
      console.error('SQL:', sql);
      console.error('Params:', allParams);
      return res.status(500).json({ error: 'Erro ao buscar dados', details: err.message });
    }
    
    console.log(`📊 Estatísticas retornadas:`, {
      total_leads: row.total_leads || 0,
      leads: row.leads || 0,
      leads_confirmados: row.leads_confirmados || 0,
      leads_cancelados: row.leads_cancelados || 0,
      leads_trash: row.leads_trash || 0,
      total_payout: row.total_payout || 0,
      filtro_offerId: offerIdFilter || 'nenhum',
      filtro_data: dateFilter || startDate || endDate || 'nenhum'
    });
    
    res.json({
      total_leads: row.total_leads || 0,
      leads: row.leads || 0,
      leads_confirmados: row.leads_confirmados || 0,
      leads_cancelados: row.leads_cancelados || 0,
      leads_trash: row.leads_trash || 0,
      total_payout: row.total_payout || 0
    });
  });
});

// Rota API para buscar todos os leads de um dia específico
app.get('/api/leads/:date', (req, res) => {
  if (!db) {
    console.error('❌ Banco de dados não está disponível');
    return res.status(500).json({ error: 'Banco de dados não disponível' });
  }

  const dateParam = req.params.date;
  const normalizedDate = normalizeDate(dateParam);
  
  if (!normalizedDate) {
    return res.status(400).json({ error: 'Data inválida. Use formato YYYY-MM-DD' });
  }

  // Buscar todos os leads do dia, mostrando apenas o status mais recente de cada lead único
  const sql = `
    WITH unique_leads AS (
      SELECT
        c.*,
        COALESCE(
          c.lead_id,  -- Prioridade 1: lead_id é único por lead
          'unique_' || CAST(c.id AS TEXT)  -- Prioridade 2: cada registro sem lead_id é único (cada lead = 1 registro)
        ) as unique_id
      FROM conversions c
      WHERE (date(c.date) = date(?) OR (c.date IS NULL AND date(c.created_at) = date(?)))
    ),
    latest_leads AS (
      SELECT ul.*
      FROM unique_leads ul
      WHERE ul.id = (
        SELECT ul2.id
        FROM unique_leads ul2
        WHERE ul2.unique_id = ul.unique_id
        ORDER BY ul2.created_at DESC
        LIMIT 1
      )
    )
    SELECT 
      id,
      COALESCE(NULLIF(TRIM(campanha), ''), sub_id6, sub_id3, 'Sem Campanha') as campanha,
      COALESCE(NULLIF(TRIM(conjunto), ''), sub_id5, 'Sem Conjunto') as conjunto,
      COALESCE(NULLIF(TRIM(anuncio), ''), sub_id4, 'Sem Anúncio') as anuncio,
      offer_id,
      status,
      payout,
      date,
      notification_type,
      created_at,
      sub_id1,
      sub_id2,
      sub_id3,
      sub_id4,
      sub_id5,
      sub_id6
    FROM latest_leads
    ORDER BY created_at DESC
  `;
  
  db.all(sql, [normalizedDate, normalizedDate], (err, rows) => {
    if (err) {
      console.error('❌ Erro ao buscar leads do dia:', err.message);
      return res.status(500).json({ error: 'Erro ao buscar dados', details: err.message });
    }
    
    console.log(`📊 Total de leads encontrados para ${normalizedDate}: ${rows.length}`);
    
    res.json({
      date: normalizedDate,
      total: rows.length,
      leads: rows
    });
  });
});

// Rota API para buscar datas com conversões (para o calendário)
app.get('/api/conversions/dates', (req, res) => {
  if (!db) {
    console.error('❌ Banco de dados não está disponível');
    return res.status(500).json({ error: 'Banco de dados não disponível' });
  }

  // Buscar todas as datas únicas que têm conversões
  // SQLite usa date() para extrair apenas a parte da data (YYYY-MM-DD)
  const sql = `
    SELECT 
      COALESCE(
        CASE WHEN date IS NOT NULL AND date != '' THEN date(date) ELSE NULL END,
        date(created_at)
      ) as date,
      COUNT(*) as count,
      SUM(CASE WHEN notification_type = 'conversao' OR notification_type = 'approval' THEN 1 ELSE 0 END) as conversoes,
      SUM(CASE WHEN notification_type = 'lead' THEN 1 ELSE 0 END) as leads,
      SUM(COALESCE(payout, 0)) as total_payout
    FROM conversions
    GROUP BY COALESCE(
      CASE WHEN date IS NOT NULL AND date != '' THEN date(date) ELSE NULL END,
      date(created_at)
    )
    ORDER BY date DESC
  `;
  
  db.all(sql, [], (err, rows) => {
    if (err) {
      console.error('Erro ao buscar datas:', err.message);
      return res.status(500).json({ error: 'Erro ao buscar dados' });
    }
    
    res.json(rows);
  });
});

// Rota API para buscar estatísticas por campanha
app.get('/api/campaign-stats', (req, res) => {
  // Verificar se banco está disponível
  if (!db) {
    console.error('❌ Banco de dados não está disponível');
    return res.status(500).json({ error: 'Banco de dados não disponível' });
  }

  const sql = `SELECT * FROM campaign_stats ORDER BY campanha, conjunto, anuncio`;
  
  db.all(sql, [], (err, rows) => {
    if (err) {
      console.error('Erro ao buscar estatísticas:', err.message);
      return res.status(500).json({ error: 'Erro ao buscar dados' });
    }
    
    res.json(rows);
  });
});

// Rota de diagnóstico para verificar campanha específica
// Rota de diagnóstico completa para testar cada etapa da query de hierarquia
app.get('/api/diagnostic/hierarchy-step-by-step', (req, res) => {
  if (!db) {
    return res.status(500).json({ error: 'Banco de dados não disponível' });
  }

  const targetDate = req.query.date || getTodayDate();
  const leadId = req.query.leadId || 'NtdNN845xN294tdbqpujKR'; // ID do lead da campanha 24
  
  console.log(`🔍 [DIAGNOSTIC] Testando hierarquia passo a passo para data: ${targetDate}, leadId: ${leadId}`);

  const results = {
    targetDate,
    leadId,
    steps: {}
  };

  // ETAPA 1: Verificar se o lead existe no banco
  db.all(`SELECT id, campanha, sub_id6, sub_id3, date, created_at, notification_type, lead_id, COALESCE(lead_id, 'unique_' || CAST(id AS TEXT)) as unique_id FROM conversions WHERE lead_id = ? OR id = 91`, 
    [leadId], (err1, step1) => {
      if (err1) {
        return res.status(500).json({ error: 'Erro na etapa 1', details: err1.message });
      }
      results.steps.step1_raw_lead = step1;
      console.log(`✅ ETAPA 1: Lead encontrado no banco:`, step1);

      // ETAPA 2: Verificar se o lead está em leads_do_dia
      db.all(`
        SELECT DISTINCT
          COALESCE(c.lead_id, 'unique_' || CAST(c.id AS TEXT)) as unique_id
        FROM conversions c
        WHERE (
          (c.date IS NOT NULL AND substr(c.date, 1, 10) = ?)
          OR (c.date IS NULL AND substr(c.created_at, 1, 10) = ?)
        )
        AND (c.lead_id = ? OR c.id = 91)
      `, [targetDate, targetDate, leadId], (err2, step2) => {
        if (err2) {
          return res.status(500).json({ error: 'Erro na etapa 2', details: err2.message });
        }
        results.steps.step2_leads_do_dia = step2;
        console.log(`✅ ETAPA 2: Lead em leads_do_dia:`, step2);

        // ETAPA 3: Verificar se o lead está em todas_conversoes
        db.all(`
          SELECT 
            c.*,
            COALESCE(c.lead_id, 'unique_' || CAST(c.id AS TEXT)) as unique_id,
            CASE 
              WHEN c.campanha IS NOT NULL AND TRIM(c.campanha) != '' AND LOWER(TRIM(c.campanha)) != 'n/a' AND LOWER(TRIM(c.campanha)) != 'untracked' THEN TRIM(c.campanha)
              WHEN c.sub_id6 IS NOT NULL AND TRIM(c.sub_id6) != '' AND LOWER(TRIM(c.sub_id6)) != 'n/a' AND LOWER(TRIM(c.sub_id6)) != 'untracked' THEN TRIM(c.sub_id6)
              WHEN c.sub_id3 IS NOT NULL AND TRIM(c.sub_id3) != '' AND LOWER(TRIM(c.sub_id3)) != 'n/a' AND LOWER(TRIM(c.sub_id3)) != 'untracked' THEN TRIM(c.sub_id3)
              ELSE 'untracked'
            END as campanha_norm
          FROM conversions c
          WHERE (c.lead_id = ? OR c.id = 91)
        `, [leadId], (err3, step3) => {
          if (err3) {
            return res.status(500).json({ error: 'Erro na etapa 3', details: err3.message });
          }
          results.steps.step3_todas_conversoes = step3;
          console.log(`✅ ETAPA 3: Lead em todas_conversoes:`, step3);
          console.log(`   Campanha normalizada:`, step3.map(s => s.campanha_norm));

          // ETAPA 4: Verificar se o lead está em conversoes_filtradas
          if (step2.length > 0 && step3.length > 0) {
            const uniqueId = step2[0].unique_id;
            db.all(`
              SELECT tc.*
              FROM (
                SELECT 
                  c.*,
                  COALESCE(c.lead_id, 'unique_' || CAST(c.id AS TEXT)) as unique_id,
                  CASE 
                    WHEN c.campanha IS NOT NULL AND TRIM(c.campanha) != '' AND LOWER(TRIM(c.campanha)) != 'n/a' AND LOWER(TRIM(c.campanha)) != 'untracked' THEN TRIM(c.campanha)
                    WHEN c.sub_id6 IS NOT NULL AND TRIM(c.sub_id6) != '' AND LOWER(TRIM(c.sub_id6)) != 'n/a' AND LOWER(TRIM(c.sub_id6)) != 'untracked' THEN TRIM(c.sub_id6)
                    WHEN c.sub_id3 IS NOT NULL AND TRIM(c.sub_id3) != '' AND LOWER(TRIM(c.sub_id3)) != 'n/a' AND LOWER(TRIM(c.sub_id3)) != 'untracked' THEN TRIM(c.sub_id3)
                    ELSE 'untracked'
                  END as campanha_norm
                FROM conversions c
                WHERE (c.lead_id = ? OR c.id = 91)
              ) tc
              INNER JOIN (
                SELECT DISTINCT
                  COALESCE(c.lead_id, 'unique_' || CAST(c.id AS TEXT)) as unique_id
                FROM conversions c
                WHERE (
                  (c.date IS NOT NULL AND substr(c.date, 1, 10) = ?)
                  OR (c.date IS NULL AND substr(c.created_at, 1, 10) = ?)
                )
              ) ldd ON ldd.unique_id = tc.unique_id
            `, [leadId, targetDate, targetDate], (err4, step4) => {
              if (err4) {
                return res.status(500).json({ error: 'Erro na etapa 4', details: err4.message });
              }
              results.steps.step4_conversoes_filtradas = step4;
              console.log(`✅ ETAPA 4: Lead em conversoes_filtradas:`, step4);

              // ETAPA 5: Verificar se o lead está em latest_conversoes
              if (step4.length > 0) {
                db.all(`
                  SELECT cf.*
                  FROM (
                    SELECT tc.*
                    FROM (
                      SELECT 
                        c.*,
                        COALESCE(c.lead_id, 'unique_' || CAST(c.id AS TEXT)) as unique_id,
                        CASE 
                          WHEN c.campanha IS NOT NULL AND TRIM(c.campanha) != '' AND LOWER(TRIM(c.campanha)) != 'n/a' AND LOWER(TRIM(c.campanha)) != 'untracked' THEN TRIM(c.campanha)
                          WHEN c.sub_id6 IS NOT NULL AND TRIM(c.sub_id6) != '' AND LOWER(TRIM(c.sub_id6)) != 'n/a' AND LOWER(TRIM(c.sub_id6)) != 'untracked' THEN TRIM(c.sub_id6)
                          WHEN c.sub_id3 IS NOT NULL AND TRIM(c.sub_id3) != '' AND LOWER(TRIM(c.sub_id3)) != 'n/a' AND LOWER(TRIM(c.sub_id3)) != 'untracked' THEN TRIM(c.sub_id3)
                          ELSE 'untracked'
                        END as campanha_norm
                      FROM conversions c
                      WHERE (c.lead_id = ? OR c.id = 91)
                    ) tc
                    INNER JOIN (
                      SELECT DISTINCT
                        COALESCE(c.lead_id, 'unique_' || CAST(c.id AS TEXT)) as unique_id
                      FROM conversions c
                      WHERE (
                        (c.date IS NOT NULL AND substr(c.date, 1, 10) = ?)
                        OR (c.date IS NULL AND substr(c.created_at, 1, 10) = ?)
                      )
                    ) ldd ON ldd.unique_id = tc.unique_id
                  ) cf
                  WHERE cf.id = (
                    SELECT cf2.id
                    FROM (
                      SELECT tc.*
                      FROM (
                        SELECT 
                          c.*,
                          COALESCE(c.lead_id, 'unique_' || CAST(c.id AS TEXT)) as unique_id
                        FROM conversions c
                        WHERE (c.lead_id = ? OR c.id = 91)
                      ) tc
                      INNER JOIN (
                        SELECT DISTINCT
                          COALESCE(c.lead_id, 'unique_' || CAST(c.id AS TEXT)) as unique_id
                        FROM conversions c
                        WHERE (
                          (c.date IS NOT NULL AND substr(c.date, 1, 10) = ?)
                          OR (c.date IS NULL AND substr(c.created_at, 1, 10) = ?)
                        )
                      ) ldd ON ldd.unique_id = tc.unique_id
                    ) cf2
                    WHERE cf2.unique_id = cf.unique_id
                    ORDER BY cf2.created_at DESC
                    LIMIT 1
                  )
                `, [leadId, targetDate, targetDate, leadId, targetDate, targetDate], (err5, step5) => {
                  if (err5) {
                    return res.status(500).json({ error: 'Erro na etapa 5', details: err5.message });
                  }
                  results.steps.step5_latest_conversoes = step5;
                  console.log(`✅ ETAPA 5: Lead em latest_conversoes:`, step5);

                  // ETAPA 6: Verificar resultado final da query completa
                  db.all(`
                    WITH leads_do_dia AS (
                      SELECT DISTINCT
                        COALESCE(c.lead_id, 'unique_' || CAST(c.id AS TEXT)) as unique_id
                      FROM conversions c
                      WHERE (
                        (c.date IS NOT NULL AND substr(c.date, 1, 10) = ?)
                        OR (c.date IS NULL AND substr(c.created_at, 1, 10) = ?)
                      )
                    ),
                    todas_conversoes AS (
                      SELECT 
                        c.*,
                        COALESCE(c.lead_id, 'unique_' || CAST(c.id AS TEXT)) as unique_id,
                        CASE 
                          WHEN c.campanha IS NOT NULL AND TRIM(c.campanha) != '' AND LOWER(TRIM(c.campanha)) != 'n/a' AND LOWER(TRIM(c.campanha)) != 'untracked' THEN TRIM(c.campanha)
                          WHEN c.sub_id6 IS NOT NULL AND TRIM(c.sub_id6) != '' AND LOWER(TRIM(c.sub_id6)) != 'n/a' AND LOWER(TRIM(c.sub_id6)) != 'untracked' THEN TRIM(c.sub_id6)
                          WHEN c.sub_id3 IS NOT NULL AND TRIM(c.sub_id3) != '' AND LOWER(TRIM(c.sub_id3)) != 'n/a' AND LOWER(TRIM(c.sub_id3)) != 'untracked' THEN TRIM(c.sub_id3)
                          ELSE 'untracked'
                        END as campanha_norm,
                        CASE 
                          WHEN c.conjunto IS NOT NULL AND TRIM(c.conjunto) != '' AND LOWER(TRIM(c.conjunto)) != 'n/a' THEN TRIM(c.conjunto)
                          WHEN c.sub_id5 IS NOT NULL AND TRIM(c.sub_id5) != '' AND LOWER(TRIM(c.sub_id5)) != 'n/a' THEN TRIM(c.sub_id5)
                          ELSE 'untracked'
                        END as conjunto_norm,
                        CASE 
                          WHEN c.anuncio IS NOT NULL AND TRIM(c.anuncio) != '' AND LOWER(TRIM(c.anuncio)) != 'n/a' THEN TRIM(c.anuncio)
                          WHEN c.sub_id4 IS NOT NULL AND TRIM(c.sub_id4) != '' AND LOWER(TRIM(c.sub_id4)) != 'n/a' THEN TRIM(c.sub_id4)
                          ELSE 'untracked'
                        END as anuncio_norm
                      FROM conversions c
                      WHERE 1=1
                    ),
                    conversoes_filtradas AS (
                      SELECT tc.*
                      FROM todas_conversoes tc
                      INNER JOIN leads_do_dia ldd ON ldd.unique_id = tc.unique_id
                    ),
                    latest_conversoes AS (
                      SELECT cf.*
                      FROM conversoes_filtradas cf
                      WHERE cf.id = (
                        SELECT cf2.id
                        FROM conversoes_filtradas cf2
                        WHERE cf2.unique_id = cf.unique_id
                        ORDER BY cf2.created_at DESC
                        LIMIT 1
                      )
                    )
                    SELECT 
                      campanha_norm as campanha,
                      conjunto_norm as conjunto,
                      anuncio_norm as anuncio,
                      COUNT(DISTINCT unique_id) as total,
                      COUNT(DISTINCT unique_id) as leads,
                      SUM(CASE WHEN notification_type = 'conversao' OR notification_type = 'approval' THEN 1 ELSE 0 END) as conversoes,
                      SUM(CASE WHEN notification_type = 'cancel' OR notification_type = 'rejection' THEN 1 ELSE 0 END) as cancelados,
                      SUM(CASE WHEN notification_type = 'trash' THEN 1 ELSE 0 END) as trash,
                      SUM(CASE WHEN notification_type = 'conversao' OR notification_type = 'approval' THEN COALESCE(payout, 0) ELSE 0 END) as total_payout
                    FROM latest_conversoes
                    WHERE campanha_norm LIKE '%Camp24%' OR campanha_norm LIKE '%121CBO%'
                    GROUP BY campanha_norm, conjunto_norm, anuncio_norm
                    ORDER BY campanha_norm, conjunto_norm, anuncio_norm
                  `, [targetDate, targetDate], (err6, step6) => {
                    if (err6) {
                      return res.status(500).json({ error: 'Erro na etapa 6', details: err6.message });
                    }
                    results.steps.step6_final_query = step6;
                    console.log(`✅ ETAPA 6: Resultado final da query:`, step6);

                    res.json(results);
                  });
                });
              } else {
                results.steps.step4_conversoes_filtradas = [];
                results.steps.step5_latest_conversoes = [];
                results.steps.step6_final_query = [];
                console.log(`❌ ETAPA 4: Lead NÃO está em conversoes_filtradas`);
                res.json(results);
              }
            });
          } else {
            results.steps.step4_conversoes_filtradas = [];
            results.steps.step5_latest_conversoes = [];
            results.steps.step6_final_query = [];
            console.log(`❌ ETAPA 2 ou 3 falhou: step2=${step2.length}, step3=${step3.length}`);
            res.json(results);
          }
        });
      });
    });
});

app.get('/api/diagnostic/campaign/:campaignName', (req, res) => {
  if (!db) {
    return res.status(500).json({ error: 'Banco de dados não disponível' });
  }

  const campaignName = req.params.campaignName;
  const today = getTodayDate();
  
  // Buscar todos os registros dessa campanha de hoje
  const sql = `
    SELECT 
      id,
      campanha,
      sub_id6,
      sub_id3,
      conjunto,
      sub_id5,
      anuncio,
      sub_id4,
      date,
      created_at,
      notification_type,
      lead_id,
      offer_id,
      COALESCE(lead_id, 'unique_' || CAST(id AS TEXT)) as unique_id
    FROM conversions
    WHERE (
      (date(date) = date(?) OR (date IS NULL AND date(created_at) = date(?)))
      AND (
        campanha LIKE ? 
        OR sub_id6 LIKE ? 
        OR sub_id3 LIKE ?
      )
    )
    ORDER BY created_at DESC
  `;
  
  const searchPattern = `%${campaignName}%`;
  
  db.all(sql, [today, today, searchPattern, searchPattern, searchPattern], (err, rows) => {
    if (err) {
      console.error('❌ Erro ao buscar campanha:', err.message);
      return res.status(500).json({ error: 'Erro ao buscar dados', details: err.message });
    }
    
    res.json({
      date: today,
      campaign_search: campaignName,
      total_found: rows.length,
      leads: rows
    });
  });
});

// Rota de diagnóstico para verificar leads recebidos hoje
app.get('/api/diagnostic/today', (req, res) => {
  if (!db) {
    return res.status(500).json({ error: 'Banco de dados não disponível' });
  }

  const today = getTodayDate();
  console.log(`🔍 Diagnóstico: Verificando leads de hoje (${today})`);

  // Contar todos os registros de hoje
  const sqlAll = `
    SELECT 
      COUNT(*) as total_registros,
      SUM(CASE WHEN notification_type = 'lead' THEN 1 ELSE 0 END) as total_leads,
      SUM(CASE WHEN notification_type = 'conversao' OR notification_type = 'approval' THEN 1 ELSE 0 END) as total_conversoes,
      SUM(CASE WHEN notification_type = 'cancel' OR notification_type = 'rejection' THEN 1 ELSE 0 END) as total_cancelados,
      SUM(CASE WHEN notification_type = 'trash' THEN 1 ELSE 0 END) as total_trash
    FROM conversions
    WHERE date(date) = date(?) OR (date IS NULL AND date(created_at) = date(?))
  `;

  // Contar leads únicos de hoje (usando a mesma lógica da query de stats)
  const sqlUnique = `
    SELECT 
      COUNT(DISTINCT unique_id) as leads_unicos,
      SUM(CASE WHEN notification_type = 'lead' THEN 1 ELSE 0 END) as leads_unicos_tipo_lead,
      SUM(CASE WHEN notification_type = 'conversao' OR notification_type = 'approval' THEN 1 ELSE 0 END) as conversoes_unicas,
      SUM(CASE WHEN notification_type = 'cancel' OR notification_type = 'rejection' THEN 1 ELSE 0 END) as cancelados_unicos,
      SUM(CASE WHEN notification_type = 'trash' THEN 1 ELSE 0 END) as trash_unicos
    FROM (
      SELECT 
        c.*,
        COALESCE(
          c.lead_id,  -- Prioridade 1: lead_id é único por lead
          'unique_' || CAST(c.id AS TEXT)  -- Prioridade 2: cada registro sem lead_id é único (cada lead = 1 registro)
        ) as unique_id
      FROM conversions c
      WHERE (date(c.date) = date(?) OR (c.date IS NULL AND date(c.created_at) = date(?)))
      AND c.id = (
        SELECT c2.id
        FROM conversions c2
        WHERE (date(c2.date) = date(?) OR (c2.date IS NULL AND date(c2.created_at) = date(?)))
        AND COALESCE(
          c2.lead_id,
          'unique_' || CAST(c2.id AS TEXT)
        ) = COALESCE(
          c.lead_id,
          'unique_' || CAST(c.id AS TEXT)
        )
        ORDER BY c2.created_at DESC
        LIMIT 1
      )
    )
  `;

  db.get(sqlAll, [today, today], (err, allStats) => {
    if (err) {
      console.error('❌ Erro ao buscar estatísticas:', err.message);
      return res.status(500).json({ error: 'Erro ao buscar dados', details: err.message });
    }

    db.get(sqlUnique, [today, today, today, today], (err2, uniqueStats) => {
      if (err2) {
        console.error('❌ Erro ao buscar leads únicos:', err2.message);
        return res.status(500).json({ error: 'Erro ao buscar leads únicos', details: err2.message });
      }

      // Buscar últimos 10 postbacks recebidos hoje
      const sqlRecent = `
        SELECT id, notification_type, offer_id, campanha, conjunto, anuncio, date, created_at, sub_id1
        FROM conversions
        WHERE date(date) = date(?) OR (date IS NULL AND date(created_at) = date(?))
        ORDER BY created_at DESC
        LIMIT 10
      `;

      // Contar por tipo de identificação
      const sqlByType = `
        SELECT 
          COUNT(DISTINCT CASE WHEN lead_id IS NOT NULL THEN lead_id END) as com_lead_id,
          COUNT(DISTINCT CASE WHEN offer_id IS NOT NULL THEN offer_id END) as com_offer_id,
          COUNT(DISTINCT CASE WHEN lead_id IS NULL AND offer_id IS NULL THEN id END) as sem_identificador,
          COUNT(*) as total_registros_hoje
        FROM conversions
        WHERE (date(date) = date(?) OR (date IS NULL AND date(created_at) = date(?)))
      `;

      db.get(sqlByType, [today, today], (err4, byType) => {
        if (err4) {
          console.error('❌ Erro ao buscar contagem por tipo:', err4.message);
        }

        // Buscar todos os unique_ids de hoje para verificar duplicatas
        const sqlAllUniqueIds = `
          SELECT 
            COALESCE(
              lead_id,
              'unique_' || CAST(id AS TEXT)
            ) as unique_id,
            COUNT(*) as count,
            GROUP_CONCAT(DISTINCT notification_type) as tipos,
            GROUP_CONCAT(id) as ids,
            GROUP_CONCAT(DISTINCT offer_id) as offer_ids
          FROM conversions
          WHERE (date(date) = date(?) OR (date IS NULL AND date(created_at) = date(?)))
          GROUP BY unique_id
          HAVING count > 1
          ORDER BY count DESC
          LIMIT 20
        `;

        db.all(sqlAllUniqueIds, [today, today], (err5, duplicates) => {
          if (err5) {
            console.error('❌ Erro ao buscar duplicatas:', err5.message);
          }

          db.all(sqlRecent, [today, today], (err3, recent) => {
            if (err3) {
              console.error('❌ Erro ao buscar postbacks recentes:', err3.message);
            }

            res.json({
              date: today,
              all_records: {
                total: allStats.total_registros || 0,
                leads: allStats.total_leads || 0,
                conversoes: allStats.total_conversoes || 0,
                cancelados: allStats.total_cancelados || 0,
                trash: allStats.total_trash || 0
              },
          unique_leads: {
            total: uniqueStats.leads_unicos || 0,
            leads: uniqueStats.leads_unicos_tipo_lead || 0,
            conversoes: uniqueStats.conversoes_unicas || 0,
            cancelados: uniqueStats.cancelados_unicos || 0,
            trash: uniqueStats.trash_unicos || 0
          },
          by_identification_type: byType || {},
          duplicates: duplicates || [],
          recent_postbacks: recent || [],
          note: 'all_records = todos os registros salvos hoje. unique_leads = leads únicos (último status de cada lead). duplicates = leads com múltiplos registros.'
        });
          });
        });
      });
    });
  });
});

// ============================================
// ROTAS API PARA PRODUTOS
// ============================================

// Listar todos os produtos
app.get('/api/produtos', (req, res) => {
  if (!db) {
    return res.status(500).json({ error: 'Banco de dados não disponível' });
  }

  db.all('SELECT * FROM produtos ORDER BY nome_produto', [], (err, rows) => {
    if (err) {
      console.error('❌ Erro ao buscar produtos:', err.message);
      return res.status(500).json({ error: 'Erro ao buscar produtos' });
    }
    res.json(rows);
  });
});

// Listar todas as contas cadastradas nos produtos (nome_conta)
app.get('/api/contas', (req, res) => {
  if (!db) {
    return res.status(500).json({ error: 'Banco de dados não disponível' });
  }

  // Buscar contas cadastradas no campo "Nome da Conta" dos produtos
  db.all('SELECT DISTINCT nome_conta as conta FROM produtos WHERE nome_conta IS NOT NULL AND nome_conta != "" ORDER BY nome_conta', [], (err, rows) => {
    if (err) {
      console.error('❌ Erro ao buscar contas:', err.message);
      return res.status(500).json({ error: 'Erro ao buscar contas' });
    }
    res.json(rows.map(row => ({ conta: row.conta })));
  });
});

// Criar novo produto
app.post('/api/produtos', (req, res) => {
  if (!db) {
    return res.status(500).json({ error: 'Banco de dados não disponível' });
  }

  const { nome_produto, offer_id, nome_conta } = req.body;

  if (!nome_produto || !offer_id || !nome_conta) {
    return res.status(400).json({ error: 'Todos os campos são obrigatórios' });
  }

  const brazilDateTime = getBrazilDateTimeSQL();
  
  db.run(
    'INSERT INTO produtos (nome_produto, offer_id, nome_conta, created_at, updated_at) VALUES (?, ?, ?, ?, ?)',
    [nome_produto, offer_id, nome_conta, brazilDateTime, brazilDateTime],
    function(err) {
      if (err) {
        console.error('❌ Erro ao criar produto:', err.message);
        return res.status(500).json({ error: 'Erro ao criar produto' });
      }
      console.log(`✅ Produto criado: ${nome_produto} (Offer ID: ${offer_id})`);
      res.json({ success: true, id: this.lastID });
    }
  );
});

// Atualizar produto
app.put('/api/produtos/:id', (req, res) => {
  if (!db) {
    return res.status(500).json({ error: 'Banco de dados não disponível' });
  }

  const { id } = req.params;
  const { nome_produto, offer_id, nome_conta } = req.body;

  if (!nome_produto || !offer_id || !nome_conta) {
    return res.status(400).json({ error: 'Todos os campos são obrigatórios' });
  }

  const brazilDateTime = getBrazilDateTimeSQL();
  
  db.run(
    'UPDATE produtos SET nome_produto = ?, offer_id = ?, nome_conta = ?, updated_at = ? WHERE id = ?',
    [nome_produto, offer_id, nome_conta, brazilDateTime, id],
    function(err) {
      if (err) {
        console.error('❌ Erro ao atualizar produto:', err.message);
        return res.status(500).json({ error: 'Erro ao atualizar produto' });
      }
      if (this.changes === 0) {
        return res.status(404).json({ error: 'Produto não encontrado' });
      }
      console.log(`✅ Produto atualizado: ID ${id}`);
      res.json({ success: true });
    }
  );
});

// Deletar produto
app.delete('/api/produtos/:id', (req, res) => {
  if (!db) {
    return res.status(500).json({ error: 'Banco de dados não disponível' });
  }

  const { id } = req.params;
  
  db.run('DELETE FROM produtos WHERE id = ?', [id], function(err) {
    if (err) {
      console.error('❌ Erro ao deletar produto:', err.message);
      return res.status(500).json({ error: 'Erro ao deletar produto' });
    }
    if (this.changes === 0) {
      return res.status(404).json({ error: 'Produto não encontrado' });
    }
    console.log(`✅ Produto deletado: ID ${id}`);
    res.json({ success: true });
  });
});

// Rota para limpar todos os leads e conversões (apenas para testes)
app.delete('/api/clear-all', (req, res) => {
  if (!db) {
    return res.status(500).json({ error: 'Banco de dados não disponível' });
  }

  // Limpar tabela conversions
  db.run('DELETE FROM conversions', (err) => {
    if (err) {
      console.error('❌ Erro ao limpar conversions:', err.message);
      return res.status(500).json({ error: 'Erro ao limpar dados' });
    }
    
    // Limpar tabela campaign_stats
    db.run('DELETE FROM campaign_stats', (err2) => {
      if (err2) {
        console.error('❌ Erro ao limpar campaign_stats:', err2.message);
        return res.status(500).json({ error: 'Erro ao limpar estatísticas' });
      }
      
      console.log('✅ Todos os dados foram limpos!');
      res.json({ success: true, message: 'Todos os leads e conversões foram removidos' });
    });
  });
});

// Rota GET alternativa para limpar (mais fácil de testar no navegador)
app.get('/api/clear-all', (req, res) => {
  if (!db) {
    return res.status(500).json({ error: 'Banco de dados não disponível' });
  }

  // Limpar tabela conversions
  db.run('DELETE FROM conversions', (err) => {
    if (err) {
      console.error('❌ Erro ao limpar conversions:', err.message);
      return res.status(500).json({ error: 'Erro ao limpar dados' });
    }
    
    // Limpar tabela campaign_stats
    db.run('DELETE FROM campaign_stats', (err2) => {
      if (err2) {
        console.error('❌ Erro ao limpar campaign_stats:', err2.message);
        return res.status(500).json({ error: 'Erro ao limpar estatísticas' });
      }
      
      console.log('✅ Todos os dados foram limpos!');
      res.json({ success: true, message: 'Todos os leads e conversões foram removidos' });
    });
  });
});

// API para buscar métricas agrupadas por sub2
app.get('/api/metricas/sub2', (req, res) => {
  if (!db) {
    return res.status(500).json({ error: 'Banco de dados não disponível' });
  }

  const dateFilter = req.query.date || null;
  const startDate = req.query.startDate || null;
  const endDate = req.query.endDate || null;
  const offerIdFilter = req.query.offerId || null;

  console.log(`📊 [MÉTRICAS SUB2] Filtros recebidos:`, {
    dateFilter,
    startDate,
    endDate,
    offerIdFilter
  });

  // IMPORTANTE: valor_total deve ser apenas das conversões (leads aprovados)
  // Usar COUNT(DISTINCT) para contar leads únicos, não todos os registros
  // Quando filtrar por offer_id, mostrar APENAS os sub2 que têm registros com aquele offer_id
  // Incluir dados do Clarity (acessos) via LEFT JOIN
  // Agrupar dados do Clarity por sub2 primeiro para evitar duplicação
  const today = getTodayDate();
  let sql = `SELECT 
    c.sub_id2 as sub2,
    COUNT(DISTINCT COALESCE(c.lead_id, 'unique_' || CAST(c.id AS TEXT))) as total_leads,
    SUM(CASE WHEN c.notification_type = 'lead' THEN 1 ELSE 0 END) as leads,
    SUM(CASE WHEN c.notification_type = 'conversao' THEN 1 ELSE 0 END) as conversoes,
    SUM(CASE WHEN c.notification_type = 'cancel' THEN 1 ELSE 0 END) as cancel,
    SUM(CASE WHEN c.notification_type = 'trash' THEN 1 ELSE 0 END) as trash,
    SUM(CASE WHEN c.notification_type = 'conversao' AND c.payout IS NOT NULL THEN c.payout ELSE 0 END) as valor_total,
    AVG(CASE WHEN c.notification_type = 'conversao' AND c.payout IS NOT NULL THEN c.payout ELSE NULL END) as valor_medio,
    COALESCE(cl.acessos_total, 0) as acessos_clarity,
    COALESCE(cl.usuarios_unicos_total, 0) as usuarios_unicos_clarity
  FROM conversions c
  LEFT JOIN (
    SELECT sub2, SUM(acessos) as acessos_total, SUM(usuarios_unicos) as usuarios_unicos_total
    FROM clarity_data
    WHERE data_coleta = ?
    GROUP BY sub2
  ) cl ON (
    -- Match exato
    c.sub_id2 = cl.sub2 
    OR 
    -- Match normalizando sub_id2 (removendo -pr2 ou pr2 do final e normalizando underscores)
    REPLACE(REPLACE(REPLACE(c.sub_id2, '-pr2', ''), 'pr2', ''), '_', '') = REPLACE(cl.sub2, '_', '')
    OR
    -- Match normalizando sub2 do Clarity (caso tenha pr2 também)
    REPLACE(c.sub_id2, '_', '') = REPLACE(REPLACE(REPLACE(cl.sub2, '-pr2', ''), 'pr2', ''), '_', '')
    OR
    -- Match normalizando ambos (removendo -pr2/pr2 e normalizando underscores)
    REPLACE(REPLACE(REPLACE(c.sub_id2, '-pr2', ''), 'pr2', ''), '_', '') = REPLACE(REPLACE(REPLACE(cl.sub2, '-pr2', ''), 'pr2', ''), '_', '')
  )
  WHERE c.sub_id2 IS NOT NULL AND c.sub_id2 != ''
  `;

  const params = [today]; // Adicionar data para o JOIN com clarity_data

  // Filtro por Offer ID - IMPORTANTE: filtrar apenas pela oferta selecionada
  // Isso garante que só mostra os sub2 (páginas) que pertencem a essa oferta específica
  if (offerIdFilter) {
    sql += ` AND c.offer_id = ?`;
    params.push(offerIdFilter.trim()); // Remove espaços em branco
    console.log(`🔍 [MÉTRICAS SUB2] Aplicando filtro por offer_id: "${offerIdFilter}"`);
    console.log(`🔍 [MÉTRICAS SUB2] Isso vai mostrar APENAS os sub2 (páginas) da oferta ${offerIdFilter}`);
  }

  if (dateFilter) {
    sql += ` AND (date(c.date) = date(?) OR (c.date IS NULL AND date(c.created_at) = date(?)))`;
    params.push(dateFilter, dateFilter);
  } else if (startDate || endDate) {
    if (startDate) {
      sql += ` AND (date(c.date) >= date(?) OR (c.date IS NULL AND date(c.created_at) >= date(?)))`;
      params.push(startDate, startDate);
    }
    if (endDate) {
      sql += ` AND (date(c.date) <= date(?) OR (c.date IS NULL AND date(c.created_at) <= date(?)))`;
      params.push(endDate, endDate);
    }
  }

  sql += ` GROUP BY c.sub_id2 ORDER BY total_leads DESC`;

  console.log(`📝 [MÉTRICAS SUB2] SQL executado:`, sql);
  console.log(`📝 [MÉTRICAS SUB2] Parâmetros:`, params);

  db.all(sql, params, (err, rows) => {
    if (err) {
      console.error('❌ Erro ao buscar métricas por sub2:', err.message);
      console.error('SQL:', sql);
      console.error('Params:', params);
      return res.status(500).json({ error: 'Erro ao buscar dados', details: err.message });
    }

    console.log(`✅ [MÉTRICAS SUB2] ${rows.length} grupos de sub2 encontrados`);
    
    // Debug: se houver filtro por offer_id, verificar se os sub2 retornados realmente pertencem a essa oferta
    if (offerIdFilter && rows.length > 0) {
      const sub2List = rows.map(r => r.sub2).join(', ');
      console.log(`📋 [MÉTRICAS SUB2] Sub2 encontrados para oferta "${offerIdFilter}": ${sub2List}`);
      
      // Verificar se há algum sub2 que não deveria estar aqui (debug)
      db.all(`SELECT DISTINCT sub_id2, offer_id FROM conversions WHERE sub_id2 IN (${rows.map(() => '?').join(',')}) AND offer_id IS NOT NULL AND offer_id != ''`, 
        rows.map(r => r.sub2), (errDebug, debugRows) => {
          if (!errDebug) {
            const sub2ComOfferIdDiferente = debugRows.filter(r => r.offer_id !== offerIdFilter.trim());
            if (sub2ComOfferIdDiferente.length > 0) {
              console.warn(`⚠️ [MÉTRICAS SUB2] ATENÇÃO: Alguns sub2 têm offer_id diferente:`, sub2ComOfferIdDiferente);
            } else {
              console.log(`✅ [MÉTRICAS SUB2] Todos os sub2 retornados pertencem à oferta "${offerIdFilter}"`);
            }
          }
        });
    }

    // Buscar totais gerais
    // IMPORTANTE: valor_total deve ser apenas das conversões (leads aprovados)
    // Usar COUNT(DISTINCT) para contar leads únicos
    // Quando filtrar por offer_id, calcular totais APENAS dessa oferta
    let sqlTotais = `SELECT 
      COUNT(DISTINCT COALESCE(lead_id, 'unique_' || CAST(id AS TEXT))) as total_leads,
      SUM(CASE WHEN notification_type = 'lead' THEN 1 ELSE 0 END) as leads,
      SUM(CASE WHEN notification_type = 'conversao' THEN 1 ELSE 0 END) as conversoes,
      SUM(CASE WHEN notification_type = 'cancel' THEN 1 ELSE 0 END) as cancel,
      SUM(CASE WHEN notification_type = 'trash' THEN 1 ELSE 0 END) as trash,
      SUM(CASE WHEN notification_type = 'conversao' AND payout IS NOT NULL THEN payout ELSE 0 END) as valor_total
    FROM conversions
    WHERE sub_id2 IS NOT NULL AND sub_id2 != ''
    `;

    const paramsTotais = [];
    
    // Filtro por Offer ID nos totais também - garantir que só conta registros dessa oferta
    if (offerIdFilter) {
      sqlTotais += ` AND offer_id = ?`;
      paramsTotais.push(offerIdFilter.trim()); // Remove espaços em branco
      console.log(`🔍 [MÉTRICAS SUB2] Totais também filtrados por offer_id: "${offerIdFilter}"`);
    }
    
    if (dateFilter) {
      sqlTotais += ` AND (date(date) = date(?) OR (date IS NULL AND date(created_at) = date(?)))`;
      paramsTotais.push(dateFilter, dateFilter);
    } else if (startDate || endDate) {
      if (startDate) {
        sqlTotais += ` AND (date(date) >= date(?) OR (date IS NULL AND date(created_at) >= date(?)))`;
        paramsTotais.push(startDate, startDate);
      }
      if (endDate) {
        sqlTotais += ` AND (date(date) <= date(?) OR (date IS NULL AND date(created_at) <= date(?)))`;
        paramsTotais.push(endDate, endDate);
      }
    }

    console.log(`📝 [MÉTRICAS SUB2] SQL Totais:`, sqlTotais);
    console.log(`📝 [MÉTRICAS SUB2] Parâmetros Totais:`, paramsTotais);

    db.get(sqlTotais, paramsTotais, (errTotais, totais) => {
      if (errTotais) {
        console.error('❌ Erro ao buscar totais:', errTotais.message);
        console.error('SQL Totais:', sqlTotais);
        console.error('Params Totais:', paramsTotais);
        return res.status(500).json({ error: 'Erro ao buscar totais', details: errTotais.message });
      }

      console.log(`✅ [MÉTRICAS SUB2] Totais calculados:`, {
        totalLeads: totais.total_leads || 0,
        conversoes: totais.conversoes || 0,
        offerIdFilter: offerIdFilter || 'nenhum'
      });

      // Formatar os dados
      const metricas = rows.map(row => {
        const acessosClarity = row.acessos_clarity || 0;
        const conversoes = row.conversoes || 0;
        
        // Taxa de conversão baseada em acessos do Clarity (se disponível) ou totalLeads
        let taxaConversao = 0;
        if (acessosClarity > 0) {
          taxaConversao = ((conversoes / acessosClarity) * 100).toFixed(2);
        } else if (row.total_leads > 0) {
          taxaConversao = ((conversoes / row.total_leads) * 100).toFixed(2);
        }
        
        return {
          sub2: row.sub2 || 'N/A',
          totalLeads: row.total_leads,
          leads: row.leads,
          conversoes: conversoes,
          cancel: row.cancel,
          trash: row.trash,
          valorTotal: row.valor_total || 0,
          valorMedio: row.valor_medio || 0,
          acessosClarity: acessosClarity,
          taxaConversao: parseFloat(taxaConversao)
        };
      });

      res.json({ 
        success: true, 
        metricas,
        totais: {
          totalLeads: totais.total_leads || 0,
          leads: totais.leads || 0,
          conversoes: totais.conversoes || 0,
          cancel: totais.cancel || 0,
          trash: totais.trash || 0,
          valorTotal: totais.valor_total || 0
        }
      });
    });
  });
});

// API de debug: listar todos os offer_ids únicos no banco
app.get('/api/debug/offer-ids', (req, res) => {
  if (!db) {
    return res.status(500).json({ error: 'Banco de dados não disponível' });
  }

  db.all(`SELECT DISTINCT offer_id, COUNT(*) as total FROM conversions WHERE offer_id IS NOT NULL AND offer_id != '' GROUP BY offer_id ORDER BY total DESC`, [], (err, rows) => {
    if (err) {
      console.error('❌ Erro ao buscar offer_ids:', err.message);
      return res.status(500).json({ error: 'Erro ao buscar dados', details: err.message });
    }

    res.json({ 
      success: true, 
      offerIds: rows.map(r => ({
        offer_id: r.offer_id,
        total: r.total
      }))
    });
  });
});

// API de debug: listar quais sub2 pertencem a cada offer_id
app.get('/api/debug/offer-sub2', (req, res) => {
  if (!db) {
    return res.status(500).json({ error: 'Banco de dados não disponível' });
  }

  const offerIdFilter = req.query.offerId || null;

  let sql = `SELECT DISTINCT offer_id, sub_id2, COUNT(*) as total 
             FROM conversions 
             WHERE offer_id IS NOT NULL AND offer_id != '' 
             AND sub_id2 IS NOT NULL AND sub_id2 != ''`;
  
  const params = [];
  if (offerIdFilter) {
    sql += ` AND offer_id = ?`;
    params.push(offerIdFilter.trim());
  }
  
  sql += ` GROUP BY offer_id, sub_id2 ORDER BY offer_id, total DESC`;

  db.all(sql, params, (err, rows) => {
    if (err) {
      console.error('❌ Erro ao buscar offer_id -> sub2:', err.message);
      return res.status(500).json({ error: 'Erro ao buscar dados', details: err.message });
    }

    // Agrupar por offer_id
    const grouped = {};
    rows.forEach(row => {
      if (!grouped[row.offer_id]) {
        grouped[row.offer_id] = [];
      }
      grouped[row.offer_id].push({
        sub2: row.sub_id2,
        total: row.total
      });
    });

    res.json({ 
      success: true, 
      data: grouped,
      message: offerIdFilter ? `Sub2 da oferta ${offerIdFilter}` : 'Todos os offer_id e seus sub2'
    });
  });
});

// Token do Clarity (deve ser configurado via variável de ambiente em produção)
const CLARITY_TOKEN = 'eyJhbGciOiJSUzI1NiIsImtpZCI6IjQ4M0FCMDhFNUYwRDMxNjdEOTRFMTQ3M0FEQTk2RTcyRDkwRUYwRkYiLCJ0eXAiOiJKV1QifQ.eyJqdGkiOiI5NmQyMWU3Ny1kYWUyLTRlNjAtOTZkNy0yNjRlYTMwMWQ4YmEiLCJzdWIiOiIzMDk4NDg1NTg5NDEyMTA1Iiwic2NvcGUiOiJEYXRhLkV4cG9ydCIsIm5iZiI6MTc2NTE5OTM1MiwiZXhwIjo0OTE4Nzk5MzUxLCJpYXQiOjE3NjUxOTkzNTEsImlzcyI6ImNsYXJpdHkiLCJhdWQiOiJjbGFyaXR5LmRhdGEtZXhwb3J0ZXIifQ.LO7vlz4mRHRA758Hj4ov2R-4xEBBx6ARisKdx2N0-pFsg36Klic_dzl1TglRftBQcVnmPgQnH05H_xk8YaHbpSpZ304YcOwjpuhD1d_jbAN22JsqBJkw8n00m3bxBExSOxoke4FOMeep0H7zWFBAws9CaYJl6oijxym7-X19FMIcytgKI_wdFYS9GoFWwMiyibfWB2uMb9Zogp-vR1M5jpjsvK9QtrGcrjSvet-Mhl8y4gPuEEJ0ypzBLTaTV1K8Px9Jk37bzXWNGkqukATYmytl0-goo-Xvg7Uixw59RI0AEocfMOmjaYMwbYnEqyX2A5fyil54WTIQ0I0qEN1AMg';

// Função para fazer uma única requisição à API do Clarity
function fetchClarityDataSingle(numOfDays = 1, dimension1 = 'URL') {
  return new Promise((resolve, reject) => {
    const url = `https://www.clarity.ms/export-data/api/v1/project-live-insights?numOfDays=${numOfDays}&dimension1=${dimension1}`;
    
    const options = {
      headers: {
        'Authorization': `Bearer ${CLARITY_TOKEN}`,
        'Content-Type': 'application/json'
      }
    };

    https.get(url, options, (res) => {
      let data = '';

      res.on('data', (chunk) => {
        data += chunk;
      });

      res.on('end', () => {
        console.log(`📡 Resposta da API do Clarity:`);
        console.log(`   Status Code: ${res.statusCode}`);
        console.log(`   Tamanho da resposta: ${data.length} bytes`);
        console.log(`   Primeiros 500 caracteres: ${data.substring(0, 500)}`);
        
        if (res.statusCode === 200) {
          try {
            const jsonData = JSON.parse(data);
            console.log(`✅ Resposta parseada com sucesso`);
            resolve(jsonData);
          } catch (err) {
            console.error(`❌ Erro ao parsear JSON:`, err.message);
            console.error(`   Resposta completa:`, data);
            reject(new Error(`Erro ao parsear resposta: ${err.message}`));
          }
        } else {
          console.error(`❌ Erro na API do Clarity: ${res.statusCode}`);
          console.error(`   Resposta:`, data);
          reject(new Error(`Erro na API do Clarity: ${res.statusCode} - ${data.substring(0, 500)}`));
        }
      });
    }).on('error', (err) => {
      reject(new Error(`Erro na requisição: ${err.message}`));
    });
  });
}

// Função para fazer requisição à API do Clarity com paginação automática
// A API do Clarity limita a 999 registros por requisição, então fazemos múltiplas requisições
async function fetchClarityData(numOfDays = 1, dimension1 = 'URL') {
  console.log(`\n🔄 Iniciando busca de dados do Clarity (${numOfDays} dia(s))`);
  
  const allData = [];
  const processedUrls = new Map(); // Map para armazenar URL -> info (para evitar duplicatas e manter o mais recente)
  
  // Fazer requisição inicial
  let data = await fetchClarityDataSingle(numOfDays, dimension1);
  
  // Processar dados iniciais
  if (Array.isArray(data)) {
    for (const metric of data) {
      if (metric.metricName === 'Traffic' && Array.isArray(metric.information)) {
        for (const info of metric.information) {
          const url = info.URL || info.url || info.pageUrl || info.Url || null;
          if (url) {
            processedUrls.set(url, info);
          }
        }
      }
    }
  }
  
  // Verificar se retornou 999 registros (limite da API)
  let totalRecords = 0;
  if (Array.isArray(data)) {
    for (const metric of data) {
      if (metric.metricName === 'Traffic' && Array.isArray(metric.information)) {
        totalRecords = metric.information.length;
        break;
      }
    }
  }
  
  // Se retornou exatamente 999, provavelmente há mais dados
  // Vamos fazer requisições adicionais dividindo o período em partes menores
  if (totalRecords >= 999) {
    console.log(`⚠️ A API retornou ${totalRecords} registros (limite de 999 atingido).`);
    console.log(`   Fazendo requisições adicionais para obter todos os dados...`);
    
    // Estratégia: fazer múltiplas requisições dividindo o período em partes menores
    // Se numOfDays > 1, dividir em requisições de 1 dia cada
    // Se numOfDays = 1, fazer algumas requisições adicionais para tentar pegar mais dados
    const requestsToMake = numOfDays > 1 ? numOfDays : 3; // Se 1 dia, fazer 3 tentativas adicionais
    const maxAttempts = Math.min(requestsToMake, 7); // Limitar a 7 tentativas para não exceder o limite de requisições
    
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        // Se numOfDays > 1, fazer requisições de 1 dia cada
        // Se numOfDays = 1, fazer requisições de 1 dia (mesmo período, mas pode haver variação)
        const daysForRequest = numOfDays > 1 ? 1 : 1;
        
        console.log(`   📅 Tentativa ${attempt}/${maxAttempts}: buscando dados adicionais (${daysForRequest} dia(s))...`);
        const additionalData = await fetchClarityDataSingle(daysForRequest, dimension1);
        
        let foundNew = false;
        let newRecordsCount = 0;
        if (Array.isArray(additionalData)) {
          for (const metric of additionalData) {
            if (metric.metricName === 'Traffic' && Array.isArray(metric.information)) {
              for (const info of metric.information) {
                const url = info.URL || info.url || info.pageUrl || info.Url || null;
                if (url && !processedUrls.has(url)) {
                  processedUrls.set(url, info);
                  foundNew = true;
                  newRecordsCount++;
                }
              }
            }
          }
        }
        
        console.log(`   📊 Tentativa ${attempt}: ${newRecordsCount} novos registros encontrados (total: ${processedUrls.size})`);
        
        // Se não encontrou novos registros e já temos muitos, provavelmente já temos tudo
        if (!foundNew && processedUrls.size >= 999) {
          console.log(`   ✅ Não foram encontrados novos registros. Parando busca.`);
          break;
        }
        
        // Pequeno delay entre requisições para não sobrecarregar a API
        await new Promise(resolve => setTimeout(resolve, 1000));
      } catch (err) {
        console.error(`   ❌ Erro na tentativa ${attempt}:`, err.message);
        // Continuar mesmo com erro
      }
    }
  }
  
  // Construir resposta no formato esperado
  if (processedUrls.size > 0) {
    const trafficMetric = {
      metricName: 'Traffic',
      information: Array.from(processedUrls.values())
    };
    allData.push(trafficMetric);
    
    // Preservar outras métricas da resposta original (se houver)
    if (Array.isArray(data)) {
      for (const metric of data) {
        if (metric.metricName !== 'Traffic') {
          allData.push(metric);
        }
      }
    }
  } else {
    // Se não processou nada, retornar dados originais
    allData.push(...(Array.isArray(data) ? data : []));
  }
  
  console.log(`✅ Total de registros únicos coletados: ${processedUrls.size}`);
  return allData;
}

// Função para extrair identificador da URL do Clarity
// Exemplo: https://news.wellhubus.com/ldr/gota/f01/vsl/gta-vsl2-ld1/index.php -> gta-vsl2-ld1
function extractIdentifierFromClarityUrl(url) {
  if (!url) return null;
  
  try {
    const urlObj = new URL(url);
    const pathname = urlObj.pathname;
    
    // Extrair o identificador do caminho da URL
    // Padrão: /ldr/gota/f01/vsl/gta-vsl2-ld1/index.php
    // Ou: /ldr/gota/f01/vsl/gta-vsl2-mn3/index.php
    const match = pathname.match(/\/([^\/]+)\/index\.php$/);
    if (match) {
      return match[1]; // Retorna: gta-vsl2-ld1 ou gta-vsl2-mn3
    }
    
    // Fallback: tentar extrair da última parte do caminho antes de index.php
    const parts = pathname.split('/').filter(p => p);
    if (parts.length > 0) {
      const lastPart = parts[parts.length - 2]; // Penúltima parte antes de index.php
      if (lastPart && !lastPart.includes('index')) {
        return lastPart;
      }
    }
    
    return null;
  } catch (err) {
    // Se não for uma URL válida, tentar extrair com regex
    const match = url.match(/\/([^\/]+)\/index\.php/);
    return match ? match[1] : null;
  }
}

// Função para normalizar sub2 removendo -pr2 ou pr2 no final
// Exemplo: gta-vsl2-ld1-pr2 -> gta-vsl2-ld1
// Exemplo: gta-vsl2-mn2pr2 -> gta-vsl2-mn2 (mas na verdade deveria ser gta-vsl2-mn3 baseado no exemplo)
function normalizeSub2(sub2) {
  if (!sub2) return null;
  
  // Remover -pr2 no final
  let normalized = sub2.replace(/-pr2$/, '');
  
  // Remover pr2 no final (sem hífen)
  normalized = normalized.replace(/pr2$/, '');
  
  return normalized;
}

// Função para extrair sub2 da URL (mantida para compatibilidade com outros formatos)
function extractSub2FromUrl(url) {
  if (!url) return null;
  
  // Primeiro tentar extrair do caminho da URL (formato Clarity)
  const identifier = extractIdentifierFromClarityUrl(url);
  if (identifier) {
    return identifier;
  }
  
  // Tentar extrair sub2 de parâmetros de query (formato tradicional)
  try {
    const urlObj = new URL(url);
    const sub2 = urlObj.searchParams.get('sub2') || urlObj.searchParams.get('sub_id2') || urlObj.searchParams.get('sub_id_2');
    return sub2;
  } catch (err) {
    // Se não for uma URL válida, tentar extrair de outros formatos
    const match = url.match(/[?&](?:sub2|sub_id2|sub_id_2)=([^&]+)/i);
    return match ? match[1] : null;
  }
}

// ============================================
// GOOGLE ANALYTICS - FUNÇÕES
// ============================================
// Configurações do Google Analytics (deve ser configurado via variável de ambiente em produção)
// O Property ID pode ser encontrado em: Admin > Property Settings > Property ID
const GA_PROPERTY_ID = process.env.GA_PROPERTY_ID || '514283157'; // ID da propriedade GA4
const GA_CREDENTIALS = process.env.GA_CREDENTIALS || ''; // JSON string das credenciais do Service Account
const GA_BASE_URL = process.env.GA_BASE_URL || 'https://news.wellhubus.com'; // URL base do site

// Função para encontrar arquivo de credenciais automaticamente
function findCredentialsFile() {
  // Primeiro, tentar usar variável de ambiente
  if (GA_CREDENTIALS) {
    // Se for um caminho de arquivo válido
    if (fs.existsSync(GA_CREDENTIALS)) {
      return GA_CREDENTIALS;
    }
    // Se for JSON string, retornar null para processar depois
    try {
      JSON.parse(GA_CREDENTIALS);
      return null; // É JSON string, não arquivo
    } catch (e) {
      // Não é JSON válido nem arquivo válido
    }
  }
  
  // Procurar arquivos JSON de credenciais no diretório raiz
  const possibleFiles = [
    'pubpay-480613-eb0b8057a62f.json', // Arquivo específico encontrado
    'credentials.json',
    'ga-credentials.json',
    'google-analytics-credentials.json',
    'service-account.json'
  ];
  
  for (const filename of possibleFiles) {
    const filePath = path.join(__dirname, filename);
    if (fs.existsSync(filePath)) {
      console.log(`✅ Arquivo de credenciais encontrado: ${filename}`);
      return filePath;
    }
  }
  
  // Procurar qualquer arquivo JSON que pareça ser credenciais do Google
  try {
    const files = fs.readdirSync(__dirname);
    for (const file of files) {
      if (file.endsWith('.json') && file !== 'package.json' && file !== 'package-lock.json') {
        const filePath = path.join(__dirname, file);
        try {
          const content = JSON.parse(fs.readFileSync(filePath, 'utf8'));
          if (content.type === 'service_account' && content.client_email) {
            console.log(`✅ Arquivo de credenciais encontrado: ${file}`);
            return filePath;
          }
        } catch (e) {
          // Não é JSON válido ou não é service account
        }
      }
    }
  } catch (err) {
    console.error('Erro ao procurar arquivos JSON:', err.message);
  }
  
  return null;
}

// Função para inicializar o cliente do Google Analytics
function getGoogleAnalyticsClient() {
  try {
    let credentials = null;
    
    // Tentar obter credenciais da variável de ambiente (JSON string)
    if (GA_CREDENTIALS) {
      try {
        credentials = JSON.parse(GA_CREDENTIALS);
        console.log('✅ Credenciais carregadas da variável de ambiente');
      } catch (e) {
        // Não é JSON string, continuar para procurar arquivo
      }
    }
    
    // Se não encontrou nas variáveis de ambiente, procurar arquivo
    if (!credentials) {
      const credentialsFile = findCredentialsFile();
      if (credentialsFile) {
        try {
          credentials = JSON.parse(fs.readFileSync(credentialsFile, 'utf8'));
          console.log(`✅ Credenciais carregadas do arquivo: ${credentialsFile}`);
        } catch (err) {
          console.error(`❌ Erro ao ler arquivo de credenciais: ${err.message}`);
        }
      }
    }
    
    if (!credentials) {
      throw new Error('Credenciais do Google Analytics não encontradas. Configure GA_CREDENTIALS ou coloque um arquivo JSON no diretório raiz.');
    }
    
    // Validar se é um service account válido
    if (!credentials.type || credentials.type !== 'service_account') {
      throw new Error('Arquivo de credenciais não é um Service Account válido');
    }
    
    const auth = new google.auth.GoogleAuth({
      credentials: credentials,
      scopes: ['https://www.googleapis.com/auth/analytics.readonly']
    });
    
    console.log(`✅ Cliente do Google Analytics inicializado com sucesso`);
    console.log(`   Service Account: ${credentials.client_email}`);
    return google.analyticsdata({ version: 'v1beta', auth });
  } catch (err) {
    console.error('❌ Erro ao inicializar cliente do Google Analytics:', err.message);
    return null;
  }
}

// Função para buscar dados do Google Analytics
async function fetchGoogleAnalyticsData(startDate, endDate) {
  const client = getGoogleAnalyticsClient();
  if (!client) {
    throw new Error('Cliente do Google Analytics não disponível');
  }
  
  if (!GA_PROPERTY_ID) {
    throw new Error('ID da propriedade do Google Analytics não configurado');
  }
  
  try {
    console.log(`📡 Buscando dados do Google Analytics de ${startDate} até ${endDate}`);
    
    const response = await client.properties.runReport({
      property: `properties/${GA_PROPERTY_ID}`,
      requestBody: {
        dateRanges: [
          {
            startDate: startDate,
            endDate: endDate
          }
        ],
        dimensions: [
          { name: 'pagePath' }, // Caminho da página
          { name: 'pageTitle' }  // Título da página (opcional)
        ],
        metrics: [
          { name: 'screenPageViews' },      // Acessos (equivalente a totalSessionCount)
          { name: 'activeUsers' }           // Usuários únicos (equivalente a distantUserCount)
        ],
        limit: 100000, // Limite máximo do GA4
        keepEmptyRows: false
      }
    });
    
    const rows = response.data.rows || [];
    console.log(`✅ Google Analytics retornou ${rows.length} registros`);
    
    // Converter dados do GA4 para o formato esperado (similar ao Clarity)
    const result = [];
    for (const row of rows) {
      const dimensionValues = row.dimensionValues || [];
      const metricValues = row.metricValues || [];
      
      const pagePath = dimensionValues[0]?.value || '';
      const pageTitle = dimensionValues[1]?.value || '';
      const screenPageViews = parseInt(metricValues[0]?.value || '0');
      const activeUsers = parseInt(metricValues[1]?.value || '0');
      
      if (pagePath) {
        // Construir URL completa
        const fullUrl = pagePath.startsWith('http') ? pagePath : `${GA_BASE_URL}${pagePath}`;
        
        result.push({
          URL: fullUrl,
          url: fullUrl,
          pageUrl: fullUrl,
          totalSessionCount: screenPageViews,
          distantUserCount: activeUsers,
          pageTitle: pageTitle
        });
      }
    }
    
    return result;
  } catch (err) {
    console.error('❌ Erro ao buscar dados do Google Analytics:', err.message);
    throw err;
  }
}

// Rota para obter contador de requisições do Clarity
app.get('/api/clarity/requests-count', (req, res) => {
  if (!db) {
    return res.status(500).json({ error: 'Banco de dados não disponível' });
  }

  const today = getTodayDate();
  
  db.get('SELECT requisicoes_feitas FROM clarity_requests WHERE data = ?', [today], (err, row) => {
    if (err) {
      console.error('❌ Erro ao buscar contador de requisições:', err.message);
      return res.status(500).json({ error: 'Erro ao buscar contador', details: err.message });
    }

    const requisicoesFeitas = row ? row.requisicoes_feitas : 0;
    const requisicoesRestantes = Math.max(0, 10 - requisicoesFeitas);

    res.json({
      success: true,
      requisicoesFeitas,
      requisicoesRestantes,
      limiteDiario: 10,
      data: today
    });
  });
});

// Rota para buscar e atualizar dados do Clarity
app.post('/api/clarity/update', async (req, res) => {
  if (!db) {
    return res.status(500).json({ error: 'Banco de dados não disponível' });
  }

  const today = getTodayDate();
  const numOfDays = req.body.numOfDays || 1;

  // Verificar se ainda há requisições disponíveis
  db.get('SELECT requisicoes_feitas FROM clarity_requests WHERE data = ?', [today], (err, row) => {
    if (err) {
      console.error('❌ Erro ao verificar contador:', err.message);
      return res.status(500).json({ error: 'Erro ao verificar contador', details: err.message });
    }

    const requisicoesFeitas = row ? row.requisicoes_feitas : 0;
    
    if (requisicoesFeitas >= 10) {
      return res.status(429).json({ 
        error: 'Limite diário atingido',
        message: 'Você já fez 10 requisições hoje. O limite será resetado amanhã.',
        requisicoesFeitas,
        requisicoesRestantes: 0
      });
    }

    // Fazer requisição à API do Clarity
    fetchClarityData(numOfDays, 'URL')
      .then(async (clarityData) => {
        console.log('✅ Dados recebidos do Clarity');
        console.log('📋 Estrutura completa da resposta:', JSON.stringify(clarityData, null, 2));
        console.log('📊 Tipo:', Array.isArray(clarityData) ? 'Array' : typeof clarityData);
        console.log('📊 Tamanho:', Array.isArray(clarityData) ? clarityData.length : 'N/A');

        // Processar dados e armazenar no banco
        const insertPromises = [];
        let totalProcessados = 0;
        let totalComUrl = 0;
        
        // A resposta é um array de objetos com metricName e information
        if (!Array.isArray(clarityData)) {
          console.error('❌ Resposta do Clarity não é um array:', typeof clarityData);
          return res.status(500).json({ 
            error: 'Formato de resposta inválido',
            details: 'A resposta da API não está no formato esperado'
          });
        }
        
        for (const metric of clarityData) {
          console.log(`\n🔍 Processando métrica: ${metric.metricName || 'N/A'}`);
          
          // Buscar métrica "Traffic" conforme documentação
          if (metric.metricName === 'Traffic' && metric.information && Array.isArray(metric.information)) {
            console.log(`   📈 Encontrada métrica Traffic com ${metric.information.length} registros`);
            
            for (const info of metric.information) {
              totalProcessados++;
              
              // Quando dimension1=URL, a propriedade deve ser "URL" (maiúscula)
              // Conforme documentação: "Possible dimensions: URL"
              const url = info.URL || info.url || info.pageUrl || info.Url || null;
              
              if (!url) {
                console.log(`   ⚠️ Registro ${totalProcessados} sem URL. Propriedades disponíveis:`, Object.keys(info));
                continue;
              }
              
              totalComUrl++;
              const acessos = parseInt(info.totalSessionCount || 0) || 0;
              const usuariosUnicos = parseInt(info.distantUserCount || 0) || 0;
              
              // Extrair identificador da URL do Clarity (ex: gta-vsl2-ld1 de /vsl/gta-vsl2-ld1/index.php)
              const identifierFromUrl = extractIdentifierFromClarityUrl(url);
              
              console.log(`   📊 Processando URL: ${url}`);
              console.log(`      Identificador extraído: ${identifierFromUrl}`);
              console.log(`      Acessos (totalSessionCount): ${acessos}`);
              console.log(`      Usuários Únicos (distantUserCount): ${usuariosUnicos}`);
              console.log(`      Dados completos:`, JSON.stringify(info, null, 2));
              
              if (!identifierFromUrl) {
                console.log(`   ⚠️ Não foi possível extrair identificador da URL: ${url}`);
                continue;
              }
              
              // Armazenar o identificador extraído da URL como sub2
              // Este será usado para fazer match com sub2 da LeadRock (que pode ter -pr2 ou pr2 no final)
              const sub2 = identifierFromUrl;

              // Inserir ou atualizar dados (SQLite usa INSERT OR REPLACE ou verificação manual)
              // Primeiro verificar se já existe
              const checkSql = 'SELECT id FROM clarity_data WHERE url = ? AND data_coleta = ?';
              const insertSql = `
                INSERT INTO clarity_data (url, sub2, acessos, usuarios_unicos, data_coleta, updated_at)
                VALUES (?, ?, ?, ?, ?, datetime('now'))
              `;
              const updateSql = `
                UPDATE clarity_data 
                SET acessos = ?, usuarios_unicos = ?, sub2 = ?, updated_at = datetime('now')
                WHERE url = ? AND data_coleta = ?
              `;
              
              console.log(`   💾 Preparando para salvar: url="${url}", sub2="${sub2}", acessos=${acessos}, usuarios_unicos=${usuariosUnicos}, data="${today}"`);
              
              insertPromises.push(
                new Promise((resolve, reject) => {
                  // Verificar se já existe
                  db.get(checkSql, [url, today], (err, existing) => {
                    if (err) {
                      console.error(`   ❌ Erro ao verificar dados do Clarity para URL ${url}:`, err.message);
                      reject(err);
                      return;
                    }
                    
                      if (existing) {
                        console.log(`   🔄 Atualizando registro existente (ID: ${existing.id})`);
                        // Atualizar registro existente
                        db.run(updateSql, [acessos, usuariosUnicos, sub2, url, today], function(err) {
                        if (err) {
                          console.error(`   ❌ Erro ao atualizar dados do Clarity:`, err.message);
                          reject(err);
                        } else {
                          console.log(`   ✅ Registro atualizado com sucesso (linhas afetadas: ${this.changes})`);
                          resolve();
                        }
                      });
                      } else {
                        console.log(`   ➕ Inserindo novo registro`);
                        // Inserir novo registro
                        db.run(insertSql, [url, sub2, acessos, usuariosUnicos, today], function(err) {
                        if (err) {
                          console.error(`   ❌ Erro ao inserir dados do Clarity:`, err.message);
                          console.error(`   SQL: ${insertSql}`);
                          console.error(`   Parâmetros: [${url}, ${sub2}, ${acessos}, ${today}]`);
                          reject(err);
                        } else {
                          console.log(`   ✅ Registro inserido com sucesso (ID: ${this.lastID}, linhas afetadas: ${this.changes})`);
                          resolve();
                        }
                      });
                    }
                  });
                })
              );
            }
          }
        }
        
        // Se não encontrou métrica Traffic, listar todas as métricas disponíveis
        if (totalProcessados === 0) {
          console.log(`\n⚠️ Nenhuma métrica "Traffic" encontrada. Métricas disponíveis:`);
          clarityData.forEach((m, idx) => {
            console.log(`   ${idx + 1}. ${m.metricName || 'Sem nome'} - ${m.information ? m.information.length : 0} registros`);
          });
        }

        console.log(`\n📊 Resumo do processamento:`);
        console.log(`   Total de registros processados: ${totalProcessados}`);
        console.log(`   Registros com URL: ${totalComUrl}`);
        console.log(`   Registros a inserir/atualizar: ${insertPromises.length}`);
        
        if (insertPromises.length === 0) {
          console.log(`\n⚠️ Nenhum registro será inserido. Verifique:`);
          console.log(`   1. Se há dados no Clarity para o período selecionado`);
          console.log(`   2. Se a dimensão URL está retornando dados`);
          console.log(`   3. Se as URLs estão no formato esperado`);
        }
        
        // Aguardar todas as inserções
        if (insertPromises.length > 0) {
          console.log(`\n💾 Iniciando salvamento de ${insertPromises.length} registros...`);
          try {
            await Promise.all(insertPromises);
            console.log(`✅ ${insertPromises.length} registros do Clarity salvos/atualizados com sucesso`);
          } catch (err) {
            console.error(`❌ Erro ao salvar registros do Clarity:`, err.message);
            console.error(`   Stack:`, err.stack);
            // Continuar mesmo com erros parciais
          }
        } else {
          console.log(`\n⚠️ Nenhum registro para salvar. Verifique os logs acima para entender o motivo.`);
        }

        // Atualizar contador de requisições
        const updateCounterSql = `
          INSERT INTO clarity_requests (data, requisicoes_feitas, updated_at)
          VALUES (?, 1, datetime('now'))
          ON CONFLICT(data) 
          DO UPDATE SET 
            requisicoes_feitas = requisicoes_feitas + 1,
            updated_at = datetime('now')
        `;

        db.run(updateCounterSql, [today], (err) => {
          if (err) {
            console.error('❌ Erro ao atualizar contador:', err.message);
          } else {
            console.log('✅ Contador de requisições atualizado');
          }
        });

        // Buscar contador atualizado
        db.get('SELECT requisicoes_feitas FROM clarity_requests WHERE data = ?', [today], (err, updatedRow) => {
          const novasRequisicoesFeitas = updatedRow ? updatedRow.requisicoes_feitas : requisicoesFeitas + 1;
          const novasRequisicoesRestantes = Math.max(0, 10 - novasRequisicoesFeitas);

          res.json({
            success: true,
            message: 'Dados do Clarity atualizados com sucesso',
            registrosInseridos: insertPromises.length,
            requisicoesFeitas: novasRequisicoesFeitas,
            requisicoesRestantes: novasRequisicoesRestantes,
            limiteDiario: 10
          });
        });
      })
      .catch((error) => {
        console.error('❌ Erro ao buscar dados do Clarity:', error.message);
        res.status(500).json({ 
          error: 'Erro ao buscar dados do Clarity',
          details: error.message
        });
      });
  });
});

// API para buscar distribuição de leads por horário
app.get('/api/metricas/horarios', (req, res) => {
  if (!db) {
    return res.status(500).json({ error: 'Banco de dados não disponível' });
  }

  const dateFilter = req.query.date || null;
  const startDate = req.query.startDate || null;
  const endDate = req.query.endDate || null;
  const offerIdFilter = req.query.offerId || null;

  let sql = `SELECT 
    substr(created_at, 12, 2) as hora,
    COUNT(*) as total_leads,
    COUNT(DISTINCT COALESCE(lead_id, 'unique_' || CAST(id AS TEXT))) as leads_unicos
  FROM conversions
  WHERE created_at IS NOT NULL
  `;

  const params = [];

  // Filtro por Offer ID
  if (offerIdFilter) {
    sql += ` AND offer_id = ?`;
    params.push(offerIdFilter);
  }

  if (dateFilter) {
    sql += ` AND (date(date) = date(?) OR (date IS NULL AND date(created_at) = date(?)))`;
    params.push(dateFilter, dateFilter);
  } else if (startDate || endDate) {
    if (startDate) {
      sql += ` AND (date(date) >= date(?) OR (date IS NULL AND date(created_at) >= date(?)))`;
      params.push(startDate, startDate);
    }
    if (endDate) {
      sql += ` AND (date(date) <= date(?) OR (date IS NULL AND date(created_at) <= date(?)))`;
      params.push(endDate, endDate);
    }
  }

  sql += ` GROUP BY hora ORDER BY hora ASC`;

  db.all(sql, params, (err, rows) => {
    if (err) {
      console.error('❌ Erro ao buscar horários:', err.message);
      return res.status(500).json({ error: 'Erro ao buscar dados', details: err.message });
    }

    // Criar array com todas as horas (00-23) e preencher com dados
    const horarios = {};
    for (let h = 0; h < 24; h++) {
      const horaStr = String(h).padStart(2, '0');
      horarios[horaStr] = {
        hora: horaStr,
        totalLeads: 0,
        leadsUnicos: 0
      };
    }

    // Preencher com dados reais
    rows.forEach(row => {
      if (row.hora && row.hora.length >= 2) {
        const hora = row.hora.substring(0, 2);
        if (horarios[hora]) {
          horarios[hora].totalLeads = row.total_leads || 0;
          horarios[hora].leadsUnicos = row.leads_unicos || 0;
        }
      }
    });

    // Converter para array
    const dadosHorarios = Object.values(horarios);

    res.json({ success: true, horarios: dadosHorarios });
  });
});

// Rota para o dashboard
app.get('/dashboard', (req, res) => {
  const indexPath = path.join(__dirname, 'public', 'index.html');
  console.log('📄 Tentando servir:', indexPath);
  res.sendFile(indexPath, (err) => {
    if (err) {
      console.error('❌ Erro ao servir index.html:', err.message);
      res.status(500).send('Erro ao carregar dashboard');
    }
  });
});

// Rota para a página de métricas
app.get('/metricas', (req, res) => {
  const metricasPath = path.join(__dirname, 'public', 'metricas.html');
  console.log('📄 Tentando servir:', metricasPath);
  res.sendFile(metricasPath, (err) => {
    if (err) {
      console.error('❌ Erro ao servir metricas.html:', err.message);
      res.status(500).send('Erro ao carregar página de métricas');
    }
  });
});

// Rota raiz redireciona para dashboard
app.get('/', (req, res) => {
  res.redirect('/dashboard');
});

// Verificar se arquivos essenciais existem
const indexPath = path.join(__dirname, 'public', 'index.html');
if (fs.existsSync(indexPath)) {
  console.log('✅ Arquivo index.html encontrado:', indexPath);
} else {
  console.error('❌ Arquivo index.html NÃO encontrado em:', indexPath);
  console.log('📁 Diretório atual:', __dirname);
  console.log('📁 Conteúdo de public:', fs.existsSync(path.join(__dirname, 'public')) ? 'existe' : 'não existe');
}

// ============================================
// ROTAS DO GOOGLE ANALYTICS
// ============================================

// Rota para obter status do Google Analytics
app.get('/api/analytics/status', (req, res) => {
  try {
    const hasPropertyId = !!GA_PROPERTY_ID && GA_PROPERTY_ID !== '';
    
    // Verificar credenciais de múltiplas formas
    let hasCredentials = false;
    let credentialsSource = 'nenhum';
    
    if (GA_CREDENTIALS) {
      hasCredentials = true;
      credentialsSource = 'variável de ambiente';
    } else {
      const credentialsFile = findCredentialsFile();
      if (credentialsFile) {
        hasCredentials = true;
        credentialsSource = `arquivo: ${path.basename(credentialsFile)}`;
      }
    }
    
    // Considerar configurado se tiver Property ID e credenciais
    const isConfigured = !!(hasPropertyId && hasCredentials);
    
    console.log(`📊 Status GA: Property ID=${hasPropertyId}, Credenciais=${hasCredentials} (${credentialsSource})`);
    
    res.json({
      success: true,
      configured: isConfigured,
      propertyId: GA_PROPERTY_ID || 'Não configurado',
      hasPropertyId: hasPropertyId,
      hasCredentials: hasCredentials,
      credentialsSource: credentialsSource,
      message: isConfigured 
        ? 'Google Analytics configurado e pronto para uso' 
        : `Configure: ${!hasPropertyId ? 'GA_PROPERTY_ID ' : ''}${!hasCredentials ? 'Credenciais (arquivo JSON ou GA_CREDENTIALS)' : ''}`
    });
  } catch (err) {
    console.error('❌ Erro ao verificar status:', err);
    res.json({
      success: false,
      configured: false,
      error: err.message,
      message: 'Erro ao verificar configuração do Google Analytics'
    });
  }
});

// Rota para buscar e atualizar dados do Google Analytics
app.post('/api/analytics/update', async (req, res) => {
  if (!db) {
    return res.status(500).json({ error: 'Banco de dados não disponível' });
  }

  const today = getTodayDate();
  const numOfDays = req.body.numOfDays || 1;
  
  // Calcular datas
  const endDate = today;
  const startDateObj = new Date(today);
  startDateObj.setDate(startDateObj.getDate() - (numOfDays - 1));
  const startDate = startDateObj.toISOString().split('T')[0];

  try {
    console.log(`\n🔄 Iniciando atualização de dados do Google Analytics`);
    console.log(`   Período: ${startDate} até ${endDate} (${numOfDays} dia(s))`);
    
    // Buscar dados do Google Analytics
    const gaData = await fetchGoogleAnalyticsData(startDate, endDate);
    
    console.log(`✅ Dados recebidos do Google Analytics: ${gaData.length} registros`);
    
    // Processar dados e armazenar no banco (usando a mesma tabela clarity_data para compatibilidade)
    const insertPromises = [];
    let totalProcessados = 0;
    let totalComUrl = 0;
    
    for (const info of gaData) {
      totalProcessados++;
      
      const url = info.URL || info.url || info.pageUrl || null;
      
      if (!url) {
        console.log(`   ⚠️ Registro ${totalProcessados} sem URL`);
        continue;
      }
      
      totalComUrl++;
      const acessos = parseInt(info.totalSessionCount || 0) || 0;
      const usuariosUnicos = parseInt(info.distantUserCount || 0) || 0;
      
      // Extrair identificador da URL (sub2) - usando a mesma função do Clarity
      const identifierFromUrl = extractIdentifierFromClarityUrl(url);
      
      console.log(`   📊 Processando URL: ${url}`);
      console.log(`      Identificador extraído: ${identifierFromUrl}`);
      console.log(`      Acessos: ${acessos}`);
      console.log(`      Usuários Únicos: ${usuariosUnicos}`);
      
      if (!identifierFromUrl) {
        console.log(`   ⚠️ Não foi possível extrair identificador da URL: ${url}`);
        continue;
      }
      
      const sub2 = identifierFromUrl;
      
      // Inserir ou atualizar dados (usando a mesma tabela clarity_data)
      // Usar INSERT OR REPLACE para evitar erro de constraint UNIQUE
      const upsertSql = `
        INSERT INTO clarity_data (url, sub2, acessos, usuarios_unicos, data_coleta, updated_at)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(url, data_coleta) 
        DO UPDATE SET 
          acessos = excluded.acessos,
          usuarios_unicos = excluded.usuarios_unicos,
          sub2 = excluded.sub2,
          updated_at = datetime('now')
      `;
      
      insertPromises.push(
        new Promise((resolve, reject) => {
          db.run(upsertSql, [url, sub2, acessos, usuariosUnicos, today], function(err) {
            if (err) {
              console.error(`   ❌ Erro ao inserir/atualizar dados para URL ${url}:`, err.message);
              reject(err);
            } else {
              if (this.changes > 0) {
                if (this.lastID) {
                  console.log(`   ✅ Registro inserido/atualizado com sucesso (ID: ${this.lastID}, changes: ${this.changes})`);
                } else {
                  console.log(`   ✅ Registro atualizado com sucesso (changes: ${this.changes})`);
                }
              } else {
                console.log(`   ⚠️ Nenhuma alteração realizada para URL ${url}`);
              }
              resolve();
            }
          });
        })
      );
    }
    
    console.log(`\n📊 Resumo do processamento:`);
    console.log(`   Total de registros processados: ${totalProcessados}`);
    console.log(`   Registros com URL: ${totalComUrl}`);
    console.log(`   Registros a inserir/atualizar: ${insertPromises.length}`);
    
    if (insertPromises.length > 0) {
      console.log(`\n💾 Iniciando salvamento de ${insertPromises.length} registros...`);
      try {
        await Promise.all(insertPromises);
        console.log(`✅ ${insertPromises.length} registros do Google Analytics salvos/atualizados com sucesso`);
      } catch (err) {
        console.error(`❌ Erro ao salvar registros:`, err.message);
        throw err;
      }
    }
    
    res.json({
      success: true,
      message: 'Dados do Google Analytics atualizados com sucesso',
      registrosInseridos: insertPromises.length,
      totalRegistros: gaData.length
    });
    
  } catch (error) {
    console.error('❌ Erro ao buscar dados do Google Analytics:', error.message);
    res.status(500).json({ 
      error: 'Erro ao buscar dados do Google Analytics',
      details: error.message
    });
  }
});

// Iniciar servidor
app.listen(PORT, () => {
  console.log(`\n🚀 Servidor rodando em http://localhost:${PORT}`);
  console.log(`📊 Dashboard: http://localhost:${PORT}/dashboard`);
  console.log(`📥 Postback URL: http://localhost:${PORT}/postback?sub_id1=...&sub_id2=...&...\n`);
});

// Fechar banco ao encerrar aplicação
process.on('SIGINT', () => {
  db.close((err) => {
    if (err) {
      console.error(err.message);
    }
    console.log('✅ Conexão com banco de dados fechada.');
    process.exit(0);
  });
});

