/**
 * Dashboard Outbound Pró Vendas — Google Apps Script
 * ─────────────────────────────────────────────────────────────────────────────
 * INSTRUÇÕES DE DEPLOY:
 * 1. Crie uma planilha no Google Drive na pasta "banco de dados"
 * 2. Abra Extensões > Apps Script
 * 3. Cole este código, salve e clique em "Executar > setupSpreadsheet" (uma vez)
 * 4. Implante como Web App:
 *    - Executar como: Eu (sua conta)
 *    - Quem tem acesso: Qualquer pessoa
 * 5. Copie a URL do Web App e coloque no .env como WEBAPP_URL
 * ─────────────────────────────────────────────────────────────────────────────
 */

// ── Configuração ──────────────────────────────────────────────────────────────
const SECRET          = 'routesales2026';
const PRESALES_PL_ID  = 1;   // ID do pipeline de Pré-Vendas no Pipedrive
const SALES_PL_ID     = 2;   // ID do pipeline de Vendas no Pipedrive

// ── Nomes das abas ────────────────────────────────────────────────────────────
const SH = {
  DEALS:      'Deals',
  ACTIVITIES: 'Activities',
  USERS:      'Users',
  STAGES:     'Stages',
  CONFIG:     'Config',
};

// ── Colunas de cada aba ───────────────────────────────────────────────────────
const COLS = {
  DEALS: [
    'id','title','pipeline_id','pipeline_name','stage_id','stage_name',
    'stage_order','status','value','currency','owner_id','owner_name',
    'org_id','org_name','person_id','person_name',
    'add_time','update_time','close_time','won_time','lost_time',
    'lost_reason','expected_close_date','weighted_value',
    'activities_count','done_activities_count','undone_activities_count',
  ],
  ACTIVITIES: [
    'id','type','subject','done','due_date','due_time','duration',
    'deal_id','person_id','org_id','user_id','note',
    'add_time','marked_as_done_time',
  ],
  USERS: ['id','name','email','role_id','active_flag'],
  STAGES: ['id','name','pipeline_id','pipeline_name','order_nr','active_flag'],
};

// ── Endpoint POST — Python envia dados aqui ───────────────────────────────────
function doPost(e) {
  try {
    const payload = JSON.parse(e.postData.contents);
    if (payload.secret !== SECRET) {
      return jsonOut({ ok: false, error: 'Unauthorized' });
    }

    const type = payload.type;   // deals | activities | users | stages
    const data = payload.data;
    const mode = payload.mode || 'replace';

    const colsMap = { deals: COLS.DEALS, activities: COLS.ACTIVITIES,
                      users: COLS.USERS,  stages: COLS.STAGES };
    const shMap   = { deals: SH.DEALS,   activities: SH.ACTIVITIES,
                      users: SH.USERS,   stages: SH.STAGES };

    if (!colsMap[type]) return jsonOut({ ok: false, error: 'Tipo inválido: ' + type });

    const updated = writeSheet(shMap[type], colsMap[type], data, mode);
    return jsonOut({ ok: true, updated: updated, type: type });

  } catch (err) {
    return jsonOut({ ok: false, error: err.toString() });
  }
}

// ── Endpoint GET — Dashboard busca dados aqui ─────────────────────────────────
function doGet(e) {
  try {
    const p      = e.parameter || {};
    const action = p.action || 'all';

    if (action === 'deals')      return jsonOut(readSheet(SH.DEALS,      COLS.DEALS));
    if (action === 'activities') return jsonOut(readSheet(SH.ACTIVITIES, COLS.ACTIVITIES));
    if (action === 'users')      return jsonOut(readSheet(SH.USERS,      COLS.USERS));
    if (action === 'stages')     return jsonOut(readSheet(SH.STAGES,     COLS.STAGES));
    if (action === 'summary')    return jsonOut(buildSummary(p));
    if (action === 'timeseries') return jsonOut(buildTimeSeries(p));

    // Default: retorna tudo de uma vez
    return jsonOut({
      ok:          true,
      deals:       readSheet(SH.DEALS,      COLS.DEALS),
      activities:  readSheet(SH.ACTIVITIES, COLS.ACTIVITIES),
      users:       readSheet(SH.USERS,      COLS.USERS),
      stages:      readSheet(SH.STAGES,     COLS.STAGES),
      lastUpdated: new Date().toISOString(),
    });

  } catch (err) {
    return jsonOut({ ok: false, error: err.toString() });
  }
}

// ── Helpers de Planilha ───────────────────────────────────────────────────────
function getOrCreate(name, headers) {
  const ss = getSpreadsheet();
  let sh = ss.getSheetByName(name);
  if (!sh) {
    sh = ss.insertSheet(name);
    if (headers && headers.length) {
      const rng = sh.getRange(1, 1, 1, headers.length);
      rng.setValues([headers])
         .setFontWeight('bold')
         .setBackground('#1a2332')
         .setFontColor('#ffffff');
      sh.setFrozenRows(1);
    }
  }
  return sh;
}

function writeSheet(shName, headers, data, mode) {
  const sh = getOrCreate(shName, headers);
  const nc = headers.length;

  if (mode === 'replace') {
    const lr = sh.getLastRow();
    if (lr > 1) sh.getRange(2, 1, lr - 1, nc).clearContent();
  }

  if (!data || !data.length) return 0;

  const rows = data.map(item =>
    headers.map(h => {
      const v = item[h];
      return (v === null || v === undefined) ? '' : v;
    })
  );

  const startRow = mode === 'replace' ? 2 : Math.max(sh.getLastRow() + 1, 2);
  sh.getRange(startRow, 1, rows.length, nc).setValues(rows);
  return rows.length;
}

function getSpreadsheet() {
  return SPREADSHEET_ID
    ? SpreadsheetApp.openById(SPREADSHEET_ID)
    : SpreadsheetApp.getActiveSpreadsheet();
}

function readSheet(shName, headers) {
  const ss = getSpreadsheet();
  const sh = ss.getSheetByName(shName);
  if (!sh) return [];
  const lr = sh.getLastRow();
  if (lr < 2) return [];
  const raw = sh.getRange(2, 1, lr - 1, headers.length).getValues();
  return raw.map(row => {
    const obj = {};
    headers.forEach((h, i) => { obj[h] = row[i] === '' ? null : row[i]; });
    return obj;
  });
}

function jsonOut(data) {
  return ContentService
    .createTextOutput(JSON.stringify(data))
    .setMimeType(ContentService.MimeType.JSON);
}

// ── Summary (KPIs) ────────────────────────────────────────────────────────────
function buildSummary(p) {
  const deals     = readSheet(SH.DEALS, COLS.DEALS);
  const dateFrom  = p.date_from ? new Date(p.date_from) : null;
  const dateTo    = p.date_to   ? new Date(p.date_to)   : null;
  const ownerId   = p.owner_id  ? parseInt(p.owner_id)  : null;

  const inRange = (dateStr, fallback) => {
    const d = dateStr ? new Date(dateStr) : null;
    if (!d) return false;
    if (dateFrom && d < dateFrom) return false;
    if (dateTo   && d > dateTo)   return false;
    return true;
  };

  const byOwner = d => !ownerId || d.owner_id == ownerId;

  const presales = deals.filter(d => d.pipeline_id == PRESALES_PL_ID && byOwner(d));
  const sales    = deals.filter(d => d.pipeline_id == SALES_PL_ID    && byOwner(d));

  // Novos leads criados no período (ambos os funis)
  const newLeads = [...presales, ...sales].filter(d => inRange(d.add_time)).length;

  // Reuniões agendadas = wins no funil de Pré-Vendas no período
  const reunioes = presales.filter(d => d.status === 'won' && inRange(d.won_time)).length;

  // Oportunidades = deals abertos no funil de Vendas (sem filtro de data — estado atual)
  const optsOpen  = sales.filter(d => d.status === 'open');
  const optsValue = optsOpen.reduce((s, d) => s + (parseFloat(d.value) || 0), 0);

  // Ganhos no funil de Vendas no período
  const won       = sales.filter(d => d.status === 'won' && inRange(d.won_time));
  const wonValue  = won.reduce((s, d) => s + (parseFloat(d.value) || 0), 0);

  // Perdidos no funil de Vendas no período
  const lost      = sales.filter(d => d.status === 'lost' && inRange(d.lost_time));
  const lostValue = lost.reduce((s, d) => s + (parseFloat(d.value) || 0), 0);

  const total = won.length + lost.length;
  const conv  = total > 0 ? Math.round(won.length / total * 1000) / 10 : 0;
  const ticket = won.length > 0 ? wonValue / won.length : 0;

  // Ciclo médio em dias
  const ciclo = won.length > 0
    ? won.reduce((s, d) => {
        if (d.add_time && d.won_time) {
          return s + (new Date(d.won_time) - new Date(d.add_time)) / 86400000;
        }
        return s;
      }, 0) / won.length
    : 0;

  return {
    ok: true,
    kpis: {
      new_leads:          newLeads,
      reunioes_agendadas: reunioes,
      oportunidades:      optsOpen.length,
      oportunidades_value: optsValue,
      ganhos:             won.length,
      ganhos_value:       wonValue,
      perdidos:           lost.length,
      perdidos_value:     lostValue,
      conversao:          conv,
      ticket_medio:       Math.round(ticket * 100) / 100,
      ciclo_medio:        Math.round(ciclo),
      em_aberto:          optsOpen.length,
    },
  };
}

// ── Time Series ───────────────────────────────────────────────────────────────
function buildTimeSeries(p) {
  const deals       = readSheet(SH.DEALS, COLS.DEALS);
  const dateFrom    = p.date_from   ? new Date(p.date_from) : new Date(Date.now() - 30 * 86400000);
  const dateTo      = p.date_to     ? new Date(p.date_to)   : new Date();
  const granularity = p.granularity || 'week'; // day | week | month
  const ownerId     = p.owner_id    ? parseInt(p.owner_id)  : null;

  const byOwner = d => !ownerId || d.owner_id == ownerId;
  const salesDeals    = deals.filter(d => d.pipeline_id == SALES_PL_ID    && byOwner(d));
  const presalesDeals = deals.filter(d => d.pipeline_id == PRESALES_PL_ID && byOwner(d));
  const allDeals      = [...salesDeals, ...presalesDeals];

  const periods = makePeriods(dateFrom, dateTo, granularity);

  const series = periods.map(per => {
    const inPer = (dateStr) => {
      const d = dateStr ? new Date(dateStr) : null;
      return d && d >= per.start && d < per.end;
    };

    return {
      label:             per.label,
      start:             per.start.toISOString(),
      end:               per.end.toISOString(),
      novos_leads:       allDeals.filter(d => inPer(d.add_time)).length,
      oportunidades:     salesDeals.filter(d => inPer(d.add_time)).length,
      novas_contas:      salesDeals.filter(d => d.status === 'won' && inPer(d.won_time)).length,
      reunioes:          presalesDeals.filter(d => d.status === 'won' && inPer(d.won_time)).length,
      valor_ganho:       salesDeals
        .filter(d => d.status === 'won' && inPer(d.won_time))
        .reduce((s, d) => s + (parseFloat(d.value) || 0), 0),
    };
  });

  return { ok: true, granularity: granularity, timeseries: series };
}

function makePeriods(start, end, gran) {
  const MONTHS = ['Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez'];
  const periods = [];
  let cur = new Date(start);

  // Align start of period
  if (gran === 'day') {
    cur.setHours(0, 0, 0, 0);
  } else if (gran === 'week') {
    const dow = cur.getDay();
    cur.setDate(cur.getDate() - (dow === 0 ? 6 : dow - 1));
    cur.setHours(0, 0, 0, 0);
  } else {
    cur = new Date(cur.getFullYear(), cur.getMonth(), 1, 0, 0, 0, 0);
  }

  while (cur < end) {
    const ps = new Date(cur);
    let pe, label;

    if (gran === 'day') {
      pe = new Date(cur); pe.setDate(pe.getDate() + 1);
      label = pad(ps.getDate()) + '/' + pad(ps.getMonth() + 1);
    } else if (gran === 'week') {
      pe = new Date(cur); pe.setDate(pe.getDate() + 7);
      label = pad(ps.getDate()) + '/' + pad(ps.getMonth() + 1);
    } else {
      pe = new Date(ps.getFullYear(), ps.getMonth() + 1, 1);
      label = MONTHS[ps.getMonth()] + '/' + String(ps.getFullYear()).slice(2);
    }

    periods.push({ start: ps, end: pe, label: label });
    cur = pe;
  }
  return periods;
}

function pad(n) { return String(n).padStart(2, '0'); }

// ── ID da planilha (banco de dados) ──────────────────────────────────────────
const SPREADSHEET_ID = '1ZqB1vrlJnjcKimp-t2ZiBqOBY5Adaa_y59Zfb69gTNk';

// ── Setup inicial (executar uma vez) ──────────────────────────────────────────
function setupSpreadsheet() {
  getOrCreate(SH.DEALS,      COLS.DEALS);
  getOrCreate(SH.ACTIVITIES, COLS.ACTIVITIES);
  getOrCreate(SH.USERS,      COLS.USERS);
  getOrCreate(SH.STAGES,     COLS.STAGES);

  const ss = getSpreadsheet();
  ss.setName('Dashboard Outbound Pró Vendas — Banco de Dados');

  // Remove a aba padrão "Plan1" se existir e não tiver dados
  const defaultSheet = ss.getSheetByName('Plan1') || ss.getSheetByName('Sheet1');
  if (defaultSheet && ss.getSheets().length > 1) {
    ss.deleteSheet(defaultSheet);
  }

  SpreadsheetApp.getUi().alert(
    '✅ Setup concluído!\n\n' +
    'Abas criadas: Deals, Activities, Users, Stages.\n\n' +
    'Agora faça o deploy como Web App e copie a URL para o arquivo .env do Python.'
  );
}
