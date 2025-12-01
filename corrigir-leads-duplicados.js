const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');

// Caminho do banco de dados
const dbPath = path.join(__dirname, 'database', 'data.db');

console.log('🔄 Iniciando correção de leads duplicados...');
console.log('📁 Caminho do banco:', dbPath);
console.log('📋 Este script corrige leads que foram duplicados em dias diferentes\n');

// Verificar se o banco existe
if (!fs.existsSync(dbPath)) {
  console.error('❌ Banco de dados não encontrado em:', dbPath);
  process.exit(1);
}

// Conectar ao banco de dados
const db = new sqlite3.Database(dbPath, sqlite3.OPEN_READWRITE, (err) => {
  if (err) {
    console.error('❌ Erro ao conectar ao banco de dados:', err.message);
    process.exit(1);
  }
  console.log('✅ Conectado ao banco de dados\n');
});

// Função para processar todas as correções
function corrigirLeadsDuplicados() {
  console.log('🔍 Buscando conversões que podem ter leads duplicados...\n');
  
  // Buscar todas as conversões (notification_type = 'conversao')
  const sql = `SELECT id, lead_id, offer_id, campanha, conjunto, anuncio, date, status, payout, created_at
               FROM conversions 
               WHERE notification_type = 'conversao'
               ORDER BY created_at DESC`;
  
  db.all(sql, [], (err, conversoes) => {
    if (err) {
      console.error('❌ Erro ao buscar conversões:', err.message);
      db.close();
      process.exit(1);
    }
    
    console.log(`✅ Encontradas ${conversoes.length} conversões para verificar\n`);
    
    if (conversoes.length === 0) {
      console.log('⚠️  Nenhuma conversão encontrada');
      db.close();
      process.exit(0);
    }
    
    let processadas = 0;
    let corrigidas = 0;
    let semCorrecao = 0;
    let erros = 0;
    
    // Processar cada conversão
    conversoes.forEach((conversao, index) => {
      // Buscar lead correspondente (status 'lead' com data anterior)
      const checkSql = `SELECT id, lead_id, offer_id, campanha, conjunto, anuncio, date, status, payout, created_at
                        FROM conversions 
                        WHERE notification_type = 'lead'
                        AND (
                          -- Mesmo lead_id
                          (? IS NOT NULL AND lead_id = ?)
                          -- OU mesmo offer_id + hierarquia
                          OR (? IS NOT NULL AND offer_id = ? 
                              AND campanha = ? 
                              AND conjunto = ? 
                              AND anuncio = ?)
                          -- OU apenas hierarquia (se não tiver IDs)
                          OR (? IS NULL AND ? IS NULL 
                              AND campanha = ? 
                              AND conjunto = ? 
                              AND anuncio = ?)
                        )
                        AND date < ?
                        ORDER BY created_at ASC
                        LIMIT 1`;
      
      db.get(checkSql, [
        conversao.lead_id, conversao.lead_id,
        conversao.offer_id, conversao.offer_id, conversao.campanha, conversao.conjunto, conversao.anuncio,
        conversao.lead_id, conversao.offer_id, conversao.campanha, conversao.conjunto, conversao.anuncio,
        conversao.date
      ], (checkErr, leadOriginal) => {
        processadas++;
        
        if (checkErr) {
          console.error(`❌ Erro ao verificar conversão #${conversao.id}:`, checkErr.message);
          erros++;
          verificarFinalizacao();
          return;
        }
        
        if (!leadOriginal) {
          // Não encontrou lead correspondente, pode ser uma conversão legítima
          semCorrecao++;
          verificarFinalizacao();
          return;
        }
        
        // Encontrou lead correspondente! Corrigir
        console.log(`\n✅ Correção encontrada:`);
        console.log(`   Lead original: ID ${leadOriginal.id} (${leadOriginal.date || 'N/A'}) - status: ${leadOriginal.notification_type}`);
        console.log(`   Conversão duplicada: ID ${conversao.id} (${conversao.date || 'N/A'}) - status: ${conversao.notification_type}`);
        console.log(`   → Vou atualizar o lead original e remover a conversão duplicada`);
        
        // Atualizar o lead original com os dados da conversão, mantendo a data original
        const updateSql = `UPDATE conversions 
                          SET notification_type = 'conversao',
                              status = COALESCE(?, status),
                              payout = COALESCE(?, payout),
                              lead_id = COALESCE(?, lead_id),
                              offer_id = COALESCE(?, offer_id)
                          WHERE id = ?`;
        
        db.run(updateSql, [
          conversao.status || leadOriginal.status,
          conversao.payout || leadOriginal.payout,
          conversao.lead_id || leadOriginal.lead_id,
          conversao.offer_id || leadOriginal.offer_id,
          leadOriginal.id
        ], function(updateErr) {
          if (updateErr) {
            console.error(`   ❌ Erro ao atualizar lead original:`, updateErr.message);
            erros++;
            verificarFinalizacao();
            return;
          }
          
          console.log(`   ✅ Lead original atualizado (ID: ${leadOriginal.id})`);
          
          // Deletar a conversão duplicada
          db.run('DELETE FROM conversions WHERE id = ?', [conversao.id], (deleteErr) => {
            if (deleteErr) {
              console.error(`   ❌ Erro ao deletar conversão duplicada:`, deleteErr.message);
              erros++;
            } else {
              console.log(`   ✅ Conversão duplicada removida (ID: ${conversao.id})`);
              corrigidas++;
            }
            
            verificarFinalizacao();
          });
        });
      });
    });
    
    // Função para verificar se terminou
    function verificarFinalizacao() {
      if (processadas === conversoes.length) {
        console.log('\n' + '='.repeat(60));
        console.log('📊 RESUMO DA CORREÇÃO:');
        console.log('='.repeat(60));
        console.log(`✅ Leads corrigidos: ${corrigidas}`);
        console.log(`⚪ Conversões sem correção necessária: ${semCorrecao}`);
        console.log(`❌ Erros: ${erros}`);
        console.log(`📊 Total processado: ${conversoes.length}`);
        console.log('='.repeat(60));
        
        if (corrigidas > 0) {
          console.log('\n✅ Correção concluída! Os leads agora estão com a data correta.');
        } else {
          console.log('\n✅ Verificação concluída! Nenhuma correção necessária.');
        }
        
        // Fechar banco
        db.close((closeErr) => {
          if (closeErr) {
            console.error('❌ Erro ao fechar banco:', closeErr.message);
          } else {
            console.log('\n✅ Banco de dados fechado com sucesso!\n');
          }
          process.exit(0);
        });
      }
    }
  });
}

// Iniciar correção
corrigirLeadsDuplicados();

