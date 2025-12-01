const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');

// Função para corrigir horário inválido (24:xx para 00:xx)
function corrigirHorario(dateTimeString) {
  if (!dateTimeString) return null;
  
  try {
    // Formato esperado: YYYY-MM-DD HH:MM:SS
    const match = dateTimeString.match(/^(\d{4}-\d{2}-\d{2}) (\d{2}):(\d{2}):(\d{2})$/);
    if (!match) return dateTimeString; // Se não está no formato esperado, retornar como está
    
    const [, datePart, hours, minutes, seconds] = match;
    let hoursInt = parseInt(hours, 10);
    let minutesInt = parseInt(minutes, 10);
    let secondsInt = parseInt(seconds, 10);
    let newDate = datePart;
    
    // Corrigir segundos se >= 60
    if (secondsInt >= 60) {
      secondsInt = secondsInt - 60;
      minutesInt = minutesInt + 1;
    }
    
    // Corrigir minutos se >= 60
    if (minutesInt >= 60) {
      minutesInt = minutesInt - 60;
      hoursInt = hoursInt + 1;
    }
    
    // Corrigir horas se >= 24
    if (hoursInt >= 24) {
      hoursInt = hoursInt - 24;
      // Ajustar data para o dia seguinte
      const dateObj = new Date(datePart + 'T00:00:00');
      dateObj.setDate(dateObj.getDate() + 1);
      const year = dateObj.getFullYear();
      const month = String(dateObj.getMonth() + 1).padStart(2, '0');
      const day = String(dateObj.getDate()).padStart(2, '0');
      newDate = `${year}-${month}-${day}`;
    }
    
    const hoursStr = String(hoursInt).padStart(2, '0');
    const minutesStr = String(minutesInt).padStart(2, '0');
    const secondsStr = String(secondsInt).padStart(2, '0');
    
    return `${newDate} ${hoursStr}:${minutesStr}:${secondsStr}`;
  } catch (err) {
    console.error('❌ Erro ao corrigir horário:', err.message);
    return dateTimeString;
  }
}

// Caminho do banco de dados
const dbPath = path.join(__dirname, 'database', 'data.db');

console.log('🔄 Iniciando correção de horários inválidos...');
console.log('📁 Caminho do banco:', dbPath);
console.log('📋 Este script corrige horários inválidos (24:xx) nos leads antigos\n');

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

// Buscar todos os leads e verificar horários inválidos
console.log('🔍 Buscando leads com horários inválidos...\n');

// Buscar todos os leads e filtrar no código (mais preciso)
const sql = `SELECT id, created_at FROM conversions 
             WHERE created_at IS NOT NULL
             ORDER BY id`;

db.all(sql, [], (err, rows) => {
  if (err) {
    console.error('❌ Erro ao buscar leads:', err.message);
    db.close();
    process.exit(1);
  }
  
  console.log(`✅ Encontrados ${rows.length} leads para verificar\n`);
  
  if (rows.length === 0) {
    console.log('✅ Nenhum lead encontrado!');
    db.close();
    process.exit(0);
  }
  
  // Filtrar apenas os que têm horários inválidos
  const leadsInvalidos = rows.filter(row => {
    if (!row.created_at) return false;
    const match = row.created_at.match(/ \d{2}:/);
    if (!match) return false;
    const hours = parseInt(match[0].trim().split(':')[0], 10);
    return hours >= 24;
  });
  
  console.log(`📊 Leads com horários inválidos encontrados: ${leadsInvalidos.length}\n`);
  
  if (leadsInvalidos.length === 0) {
    console.log('✅ Nenhum lead com horário inválido encontrado!');
    db.close();
    process.exit(0);
  }
  
  let corrigidos = 0;
  let erros = 0;
  let processados = 0;
  
  // Processar cada lead com horário inválido
  leadsInvalidos.forEach((row) => {
    const leadId = row.id;
    const createdAt = row.created_at;
    const novoHorario = corrigirHorario(createdAt);
    
    if (!novoHorario || novoHorario === createdAt) {
      processados++;
      if (processados === leadsInvalidos.length) {
        finalizar();
      }
      return;
    }
    
    // Atualizar o horário no banco
    db.run('UPDATE conversions SET created_at = ? WHERE id = ?', [novoHorario, leadId], (updateErr) => {
      processados++;
      
      if (updateErr) {
        console.error(`❌ Erro ao atualizar lead #${leadId}:`, updateErr.message);
        erros++;
      } else {
        corrigidos++;
        console.log(`✅ Lead #${leadId}: ${createdAt} → ${novoHorario}`);
      }
      
      if (processados === leadsInvalidos.length) {
        finalizar();
      }
    });
  });
  
  function finalizar() {
    console.log('\n' + '='.repeat(50));
    console.log('📊 RESUMO DA CORREÇÃO:');
    console.log('='.repeat(50));
    console.log(`✅ Horários corrigidos: ${corrigidos}`);
    console.log(`⚪ Sem alteração: ${leadsInvalidos.length - corrigidos - erros}`);
    console.log(`❌ Erros: ${erros}`);
    console.log(`📊 Total processado: ${leadsInvalidos.length}`);
    console.log('='.repeat(50));
    
    // Fechar banco
    db.close((closeErr) => {
      if (closeErr) {
        console.error('❌ Erro ao fechar banco:', closeErr.message);
      } else {
        console.log('\n✅ Banco de dados fechado com sucesso!');
        console.log('✅ Correção concluída!\n');
      }
      process.exit(0);
    });
  }
});

