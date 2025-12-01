const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');

// Função para converter uma data/hora para o fuso horário de São Paulo/Brasil e retornar apenas a data (YYYY-MM-DD)
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

// Caminho do banco de dados
const dbPath = path.join(__dirname, 'database', 'data.db');

console.log('🔄 Iniciando atualização dos leads antigos...');
console.log('📅 Convertendo datas para o horário de São Paulo/Brasil');
console.log('📁 Caminho do banco:', dbPath);

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
  console.log('✅ Conectado ao banco de dados');
});

// Buscar todos os leads
console.log('\n📊 Buscando todos os leads...');
db.all('SELECT id, created_at, date FROM conversions ORDER BY id', [], (err, rows) => {
  if (err) {
    console.error('❌ Erro ao buscar leads:', err.message);
    db.close();
    process.exit(1);
  }
  
  console.log(`✅ Encontrados ${rows.length} leads para atualizar\n`);
  
  let updated = 0;
  let unchanged = 0;
  let errors = 0;
  let processed = 0;
  
  // Função para processar um lead
  function processLead(row, index) {
    const leadId = row.id;
    const createdAt = row.created_at;
    const oldDate = row.date;
    
    // Converter created_at para data no fuso horário do Brasil
    const newDate = getDateInBrazilTimezone(createdAt);
    
    if (!newDate) {
      console.log(`⚠️  Lead #${leadId}: Não foi possível converter a data (created_at: ${createdAt})`);
      errors++;
      processed++;
      checkCompletion();
      return;
    }
    
    // Comparar com a data antiga
    if (oldDate === newDate) {
      unchanged++;
      processed++;
      
      // Mostrar progresso a cada 50 leads
      if (processed % 50 === 0) {
        console.log(`📝 Processados ${processed}/${rows.length} leads... (${updated} atualizados, ${unchanged} sem alteração)`);
      }
      
      checkCompletion();
      return;
    }
    
    // Atualizar a data
    db.run('UPDATE conversions SET date = ? WHERE id = ?', [newDate, leadId], (updateErr) => {
      processed++;
      
      if (updateErr) {
        console.error(`❌ Erro ao atualizar lead #${leadId}:`, updateErr.message);
        errors++;
      } else {
        updated++;
        console.log(`✅ Lead #${leadId}: ${oldDate || 'NULL'} → ${newDate}`);
      }
      
      // Mostrar progresso a cada 50 leads
      if (processed % 50 === 0) {
        console.log(`📝 Processados ${processed}/${rows.length} leads... (${updated} atualizados, ${unchanged} sem alteração)`);
      }
      
      checkCompletion();
    });
  }
  
  // Função para verificar se terminou
  function checkCompletion() {
    if (processed === rows.length) {
      console.log('\n' + '='.repeat(50));
      console.log('📊 RESUMO DA ATUALIZAÇÃO:');
      console.log('='.repeat(50));
      console.log(`✅ Atualizados: ${updated}`);
      console.log(`⚪ Sem alteração: ${unchanged}`);
      console.log(`❌ Erros: ${errors}`);
      console.log(`📊 Total: ${rows.length}`);
      console.log('='.repeat(50));
      
      // Fechar banco
      db.close((closeErr) => {
        if (closeErr) {
          console.error('❌ Erro ao fechar banco:', closeErr.message);
        } else {
          console.log('\n✅ Banco de dados fechado com sucesso!');
          console.log('✅ Atualização concluída!\n');
        }
        process.exit(0);
      });
    }
  }
  
  // Processar cada lead (um de cada vez para evitar sobrecarga)
  rows.forEach((row, index) => {
    processLead(row, index);
  });
  
  // Se não houver leads
  if (rows.length === 0) {
    console.log('⚠️  Nenhum lead encontrado para atualizar');
    db.close();
    process.exit(0);
  }
});

