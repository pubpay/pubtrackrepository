const sqlite3 = require('sqlite3').verbose();
const path = require('path');

// Para Node < 18 usamos node-fetch; para Node 18+ pode usar fetch nativo
let fetchFn;
try {
  // Node 18+ já tem fetch global
  if (typeof fetch !== 'undefined') {
    fetchFn = fetch;
  } else {
    // eslint-disable-next-line global-require
    fetchFn = (...args) => import('node-fetch').then(({ default: f }) => f(...args));
  }
} catch (e) {
  fetchFn = (...args) => import('node-fetch').then(({ default: f }) => f(...args));
}

// URL da API remota com dados reais
const API_URL = 'http://72.61.50.85:3000/api/extrato?date=today';

// Caminho do mesmo banco usado pelo server.js
const dbPath = path.join(__dirname, 'database', 'data.db');
const db = new sqlite3.Database(dbPath);

// Se quiser começar do zero em ambiente de teste, deixe como true
const LIMPAR_ANTES_DE_INSERIR = false;

async function importar() {
  console.log('🔄 Buscando extrato na API remota:', API_URL);

  const res = await fetchFn(API_URL);
  if (!res.ok) {
    throw new Error(`Falha ao chamar API (${res.status})`);
  }

  const dados = await res.json();
  if (!Array.isArray(dados)) {
    throw new Error('Resposta da API não é um array');
  }

  console.log(`✅ Recebidos ${dados.length} registros. Inserindo no banco: ${dbPath}`);

  const cols = [
    'sub_id1',
    'sub_id2',
    'sub_id3',
    'sub_id4',
    'sub_id5',
    'sub_id6',
    'sub_id7',
    'sub_id8',
    'campanha',
    'conjunto',
    'anuncio',
    'offer_id',
    'lead_id',
    'status',
    'payout',
    'date',
    'notification_type',
    'utm_source',
    'utm_medium',
    'created_at',
    'categoria'
  ];

  const placeholders = cols.map(() => '?').join(', ');
  const insertSql = `
    INSERT INTO conversions (
      ${cols.join(', ')}
    ) VALUES (${placeholders})
  `;

  db.serialize(() => {
    if (LIMPAR_ANTES_DE_INSERIR) {
      console.log('🧹 Limpando tabela conversions antes de importar (modo teste)...');
      db.run('DELETE FROM conversions');
    }

    db.run('BEGIN TRANSACTION');
    const stmt = db.prepare(insertSql);

    dados.forEach((row, idx) => {
      const values = cols.map(col => {
        if (col === 'payout' && row[col] == null) return 0;
        if (col === 'created_at' && !row[col]) return new Date().toISOString();
        return row[col] != null ? row[col] : null;
      });

      stmt.run(values, err => {
        if (err) {
          console.error(`❌ Erro ao inserir registro #${idx}:`, err.message);
        }
      });
    });

    stmt.finalize(err => {
      if (err) {
        console.error('❌ Erro ao finalizar inserções, dando ROLLBACK:', err.message);
        db.run('ROLLBACK');
        db.close();
        return;
      }

      db.run('COMMIT', commitErr => {
        if (commitErr) {
          console.error('❌ Erro no COMMIT:', commitErr.message);
        } else {
          console.log('✅ Importação concluída com sucesso!');
        }
        db.close();
      });
    });
  });
}

importar().catch(err => {
  console.error('❌ Erro geral na importação:', err);
  db.close();
});


