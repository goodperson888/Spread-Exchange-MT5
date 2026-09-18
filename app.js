const $ = id => document.getElementById(id);
const bindings = {
  mt5: {adapter:'mt5-adapter', symbol:'mt5-symbol', terminal_path:'terminal-path', mcp_url:'mcp-url', account:'account', server:'mt5-server'},
  strategy: {mt5_lots:'lots', entry_spread_usd:'entry', take_contraction_usd:'take', max_groups:'max-groups', max_quote_age_ms:'max-age', max_clock_skew_ms:'max-clock-skew', max_unhedged_ms:'max-unhedged', max_slippage_usd:'slippage', grid_spacing_usd:'grid-spacing', grid_max_adds:'grid-adds'},
};
const DRAFT_KEY = 'GoldPairLocal.configDraft.v2';
const LEGACY_DRAFT_KEY = 'GoldPairLocal.configDraft.v1';
const SECRET_IDS = new Set(['trading-api-key','trading-api-secret','mt5-mcp-token']);
const dirtyFields = new Set();
const dirtySecrets = new Set();
const sectionFields = {
  mt5: new Set([...Object.values(bindings.mt5), 'mt5-mcp-token']),
  binance: new Set(['symbol','recv-window','binance-proxy','trading-api-key','trading-api-secret']),
  execution: new Set(['exec-mode','poll-ms','magic','close-retries']),
};
let credentialsDirty = false;
let saveQueue = Promise.resolve();
let revision = 0, busy = false, loaded = false, hasResult = false, planTimer, saveTimer, autoSaving=false, publicIpValue='';
let pollBusy = false;
window.goldPairUiBusy = false;
const value = id => $(id).value.trim();
const message = (id, text, error=false) => {
  $(id).textContent = text;
  $(id).classList.toggle('error', error);
};
for (const button of document.querySelectorAll('[data-toggle-secret]')) {
  button.addEventListener('click', () => {
    const input=$(button.dataset.toggleSecret);
    const showing=input.type==='password';
    input.type=showing?'text':'password';
    button.setAttribute('aria-pressed',String(showing));
    const name=input.id==='trading-api-key'?'API Key':input.id==='trading-api-secret'?'Secret Key':'MCP Token';
    const label=(showing?'隐藏 ':'显示 ')+name;
    button.setAttribute('aria-label',label);
    button.title=label;
  });
}
async function api(path, body) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 30000);
  try {
  const response = await fetch(path, {
    signal: controller.signal,
    method: body === undefined ? 'GET' : 'POST',
    headers: {'Content-Type':'application/json', 'X-Local-App':'GoldPairLocal'},
    body: body === undefined ? undefined : JSON.stringify(body),
    cache: 'no-store',
  });
  const result = await response.json();
  if (!response.ok || result.ok === false) throw new Error((result.errors || [result.error || '请求失败']).join('；'));
  return result;
  } catch (error) {
    if (error.name === 'AbortError') throw new Error('请求超过 30 秒，请检查连接后重试；页面输入已保留。');
    throw error;
  } finally { clearTimeout(timer); }
}
function mt5Form() {
  return Object.fromEntries(Object.entries(bindings.mt5).map(([key,id]) => [key, value(id)]));
}
function formConfig() {
  const mt5=mt5Form();
  mt5.mcp_token=value('mt5-mcp-token');
  return {
    mode:'paper', symbol:value('symbol').toUpperCase(), mt5,
    binance:{recv_window_ms:Number(value('recv-window')),proxy_url:value('binance-proxy'),api_key:value('trading-api-key'),api_secret:value('trading-api-secret')},
    strategy:{...Object.fromEntries(Object.entries(bindings.strategy).map(([key,id]) => [key, Number(value(id))])),grid_enabled:$('grid-enabled').checked,
      cooldown_seconds:Number(value('cooldown')),max_total_lots:Number(value('max-total')),exit_mode:value('exit-mode'),target_mode:value('target-mode'),exit_spread_usd:Number(value('absolute-exit')),
      require_net_profit:$('require-net').checked,min_net_profit_usd:Number(value('min-net')),group_loss_enabled:$('group-loss-on').checked,group_max_loss_usd:Number(value('group-loss')),
      total_loss_enabled:$('total-loss-on').checked,total_max_loss_usd:Number(value('total-loss')),max_hold_minutes:Number(value('max-hold'))},
    execution:{mode:value('exec-mode'), quote_source:'market', poll_ms:Number(value('poll-ms')), magic:Number(value('magic')), close_retry_limit:Number(value('close-retries'))},
    costs:{usdt_usd_auto:$('auto-fx').checked,usdt_usd:Number(value('usdt-fx')),binance_fee_auto:$('auto-binance-fee').checked,binance_taker_percent:Number(value('binance-fee')), mt5_commission_per_lot_side:Number(value('mt5-fee')), mt5_swap_per_lot_day:Number(value('mt5-swap')), paper_funding_percent_day:Number(value('paper-funding'))},
  };
}
function editableField(id) {
  const input=$(id);
  return input && !input.readOnly && !SECRET_IDS.has(id) && id!=='chart-window' &&
    ['INPUT','SELECT'].includes(input.tagName);
}
function draftFields() {
  return Object.fromEntries([...dirtyFields].filter(editableField).map(id=>
    [id,$(id).type==='checkbox'?$(id).checked:$(id).value]));
}
function restoreFields(fields) {
  for(const [id,value] of Object.entries(fields||{})) {
    if(!editableField(id)) continue;
    if($(id).type==='checkbox') $(id).checked=value===true;
    else $(id).value=String(value??'');
    dirtyFields.add(id);
  }
}
function saveDraft() {
  if(!dirtyFields.size) return;
  try {
    // Preserve raw strings (including temporarily empty numbers) and only
    // edited fields. Credentials never enter this browser draft; they are
    // saved only through the local config endpoint.
    const previous=JSON.parse(localStorage.getItem(DRAFT_KEY)||'null');
    localStorage.setItem(DRAFT_KEY,JSON.stringify({fields:{...previous?.fields,...draftFields()}}));
  } catch (_) {
    message('config-status','浏览器草稿不可用，请等待“已保存到本机”后再刷新。',true);
  }
}
function readDraft() {
  try { return JSON.parse(localStorage.getItem(DRAFT_KEY)||'null'); }
  catch (_) { return null; }
}
function clearDraft() {
  try { localStorage.removeItem(DRAFT_KEY); localStorage.removeItem(LEGACY_DRAFT_KEY); } catch (_) {}
}
function removeDraftFields(ids) {
  try {
    const draft=JSON.parse(localStorage.getItem(DRAFT_KEY)||'null');
    if (!draft?.fields) return;
    for (const id of ids) delete draft.fields[id];
    if (Object.keys(draft.fields).length) localStorage.setItem(DRAFT_KEY,JSON.stringify(draft));
    else localStorage.removeItem(DRAFT_KEY);
  } catch (_) {}
}
function populate(c) {
  for (const [section, fields] of Object.entries(bindings)) {
    for (const [key, id] of Object.entries(fields)) $(id).value = c[section]?.[key] ?? '';
  }
  $('recv-window').value = c.binance.recv_window_ms;
  $('binance-proxy').value = c.binance.proxy_url ?? '';
  $('trading-api-key').value = c.binance.api_key ?? '';
  $('trading-api-secret').value = c.binance.api_secret ?? '';
  $('mt5-mcp-token').value = c.mt5?.mcp_token ?? '';
  $('symbol').value = c.symbol;
  $('exec-mode').value = c.execution?.mode ?? 'paper';
  // 只支持 paper 和 live 两种模式
  $('poll-ms').value = c.execution?.poll_ms ?? 250;
  $('magic').value = c.execution?.magic ?? 9121701;
  $('close-retries').value = c.execution?.close_retry_limit ?? 3;
  const s=c.strategy || {}, costs=c.costs || {};
  const extra={cooldown_seconds:'cooldown',max_total_lots:'max-total',exit_mode:'exit-mode',target_mode:'target-mode',exit_spread_usd:'absolute-exit',min_net_profit_usd:'min-net',group_max_loss_usd:'group-loss',total_max_loss_usd:'total-loss',max_hold_minutes:'max-hold'};
  for(const [key,id] of Object.entries(extra)) $(id).value=s[key] ?? $(id).value;
  for(const [key,id] of Object.entries({require_net_profit:'require-net',group_loss_enabled:'group-loss-on',total_loss_enabled:'total-loss-on',grid_enabled:'grid-enabled'})) $(id).checked=Boolean(s[key]);
  for(const [key,id] of Object.entries({usdt_usd:'usdt-fx',binance_taker_percent:'binance-fee',mt5_commission_per_lot_side:'mt5-fee',mt5_swap_per_lot_day:'mt5-swap',paper_funding_percent_day:'paper-funding'})) $(id).value=costs[key] ?? $(id).value;
  $('auto-fx').checked=costs.usdt_usd_auto ?? true;
  $('auto-binance-fee').checked=costs.binance_fee_auto ?? true;
  for(const [key,id] of [['contract_size_oz','contract'],['volume_min','volume-min'],['volume_step','volume-step']])
    $(id).value=c.mt5?.[key]??'';
  message('mt5-result','已恢复保存的目标账户与规格；连接检查状态需重新验证。');
  credentialsDirty=false; dirtySecrets.clear();
  syncConditionalFields();
}
function visible(id, show) { $(id).classList.toggle('is-hidden', !show); }
function syncConditionalFields(source='') {
  let live=value('exec-mode')==='live';
  const mcpAdapter=value('mt5-adapter')==='mcp';
  const incompatible=live && mcpAdapter;
  const compatibility=$('mode-compatibility');
  if (compatibility) {
    if (incompatible) {
      compatibility.textContent='当前组合不能实盘：MT5 MCP 只提供只读行情。页面保留你的选择；需要实盘时请改用 Windows MT5 原生终端。';
      compatibility.classList.add('error');
    } else if (live) {
      compatibility.textContent='实盘组合：Windows MT5 原生终端 + 币安 API。';
      compatibility.classList.remove('error');
    } else {
      compatibility.textContent='纸面模式可使用 MT5 MCP 真实行情，也可使用原生终端；不会发送真实订单。';
      compatibility.classList.remove('error');
    }
  }
  $('api-keys-section').style.display=live?'grid':'none';
  visible('legacy-paper-actions',!live);visible('legacy-paper-panel',!live);
  visible('paper-funding-field',!live);visible('mt5-swap-field',!live);
  visible('auto-binance-fee-field',live);visible('mcp-fields',mcpAdapter);visible('native-path-fields',!mcpAdapter);
  visible('contraction-field',value('target-mode')==='contraction');
  visible('absolute-field',value('target-mode')==='absolute');
  visible('min-net-field',$('require-net').checked);
  $('min-net-label').textContent=value('exit-mode')==='basket'?'整篮子最低净收益（USD）':'单组最低净收益（USD）';
  visible('group-loss-field',$('group-loss-on').checked);
  visible('total-loss-field',$('total-loss-on').checked);
  visible('grid-spacing-field',$('grid-enabled').checked);
  visible('grid-adds-field',$('grid-enabled').checked);
  $('usdt-fx').disabled=$('auto-fx').checked;
  $('binance-fee').disabled=live&&$('auto-binance-fee').checked;
}
function controls() {
  for (const id of ['save-config','clear-credentials','check-binance','check-mt5','check-public-ip','paper-toggle','paper-step','paper-reset']) if($(id)) $(id).disabled = busy || !loaded;
  $('copy-public-ip').disabled = busy || !loaded || !publicIpValue;
}
async function action(button, job) {
  if (busy || !loaded) return;
  busy = true; window.goldPairUiBusy = true; controls();
  const buttonElement = $(button);
  const label = buttonElement.textContent;
  buttonElement.textContent = button === 'check-mt5' ? '检查中，最多等待约 18 秒…' : button === 'check-binance' ? '正在检查行情与权限…' : button === 'check-public-ip' ? '正在检测出口…' : '处理中…';
  buttonElement.classList.add('processing');
  try { await job(); }
  catch (error) { message(button === 'check-mt5' ? 'mt5-result' : button === 'check-binance' ? 'binance-result' : button === 'check-public-ip' ? 'public-ip-result' : ['paper-step','paper-toggle','paper-reset'].includes(button) ? 'plan-result' : 'config-status', error.message, true); }
  finally { busy = false; window.goldPairUiBusy = false; buttonElement.textContent = label; buttonElement.classList.remove('processing'); controls(); }
}
function saveConfigPayload(payload, sections=null) {
  clearTimeout(saveTimer);
  if(!loaded) return Promise.reject(Error('配置尚未读取完成，请稍候再保存或连接。'));
  saveDraft();
  // All full-form saves share one queue so an older response cannot replace
  // a newer edit or clear its draft.
  saveQueue=saveQueue.catch(()=>{}).then(async()=>{
    const at=revision;
    message('config-status','正在保存到本机…');
    await api('/api/config',payload);
    if(at===revision) {
      if (sections===null) {
        dirtyFields.clear(); credentialsDirty=false; dirtySecrets.clear(); clearDraft();
      } else {
        const savedIds=new Set(sections.flatMap(section=>[...(sectionFields[section]||[])]));
        for (const id of savedIds) dirtyFields.delete(id);
        for (const id of savedIds) dirtySecrets.delete(id);
        credentialsDirty=dirtySecrets.size>0;
        removeDraftFields(savedIds);
        saveDraft();
      }
      message('config-status',sections===null?'已保存到本机，刷新后会恢复配置。':'已保存本次检查所需配置到本机。');
    } else { saveDraft(); queueAutoSave(); }
    return at;
  });
  return saveQueue;
}
function saveConfig() { return saveConfigPayload(formConfig()); }
function saveConfigSections(sections) {
  const full=formConfig(), payload={};
  for (const section of sections) if (full[section]!==undefined) payload[section]=full[section];
  if (sections.includes('binance')) payload.symbol=full.symbol;
  return saveConfigPayload(payload,sections);
}
function queueAutoSave() {
  clearTimeout(saveTimer);
  saveTimer=setTimeout(autoSaveConfig,800);
}
async function autoSaveConfig() {
  if(!loaded || busy || autoSaving) { queueAutoSave(); return; }
  if(!dirtyFields.size && !credentialsDirty) return;
  autoSaving=true;
  try { await saveConfig(); }
  catch(error) { message('config-status','尚未保存到本机：'+error.message+'；未保存输入保留在浏览器草稿中。',true); }
  finally { autoSaving=false; }
}
$('save-config').onclick=()=>action('save-config',saveConfig);
$('clear-credentials').onclick=()=>action('clear-credentials',async()=>{
  await api('/api/config',{clear_credentials:true});
  $('trading-api-key').value='';$('trading-api-secret').value='';$('mt5-mcp-token').value='';
  credentialsDirty=false; dirtySecrets.clear();
  message('config-status','已清除本机保存的 API Key、Secret 和 MCP Token。');
});
function showResult(r) {
  const lines = [r.message];
  if (r.identity) lines.push('实际账户：' + r.identity.account + ' · ' + r.identity.server + ' · ' + r.identity.currency);
  if (r.symbol && r.symbol.name) lines.push('黄金品种：' + r.symbol.name + '；每手 ' + r.symbol.contract_size_oz + ' 盎司；最小 ' + r.symbol.volume_min + ' / 步长 ' + r.symbol.volume_step + ' / 最大 ' + r.symbol.volume_max + ' 手');
  if (r.quote) lines.push('检查时 Bid ' + r.quote.bid + ' / Ask ' + r.quote.ask + '；报价年龄 ' + r.quote.age_ms + ' 毫秒');
  if (r.quote) { 
    $('mt5-bid').value = r.quote.bid; 
    $('mt5-ask').value = r.quote.ask;
  }
  for (const reason of r.blockers || []) lines.push('待处理：' + reason);
  message('mt5-result', lines.join('\n'), !r.connected || !r.identity_matches || !r.symbol);
  $('mt5-symbols').replaceChildren(...(r.candidates || []).map(name => {
    const option = document.createElement('option'); option.value = name; return option;
  }));
  
  // 设置hasResult状态
  hasResult = Boolean(r.connected && r.identity_matches && r.symbol);
}
function showPlan(p, executable=false) {
  const notional=p.binance_notional_usdt;
  message('plan-result', 'MT5 ' + (p.mt5_symbol || value('mt5-symbol')) + '：' + (p.mt5_lots ?? p.lots) + ' 手 ↔ ' + value('symbol').toUpperCase() + '：' + (p.binance_qty_xau ?? p.qty) + ' XAU\n黄金数量 ' + (p.gold_qty_oz ?? p.qty) + ' 盎司；' + (notional == null ? '连接行情后显示名义金额' : '当前名义金额约 ' + Number(notional).toFixed(2) + ' USDT（不是保证金）') + '\n' + (executable ? '已按 MT5 与币安实际数量规则校验。' : p.note));
  $('plan-state').textContent=executable?'双边规则已校验':'配平已自动计算';
  $('plan-state').classList.remove('warning');
}
async function calculatePlan(save=true) {
  if (!hasResult) return;
  try {
    if (save) await saveConfig();
    const {plan:p} = await api('/api/paper/plan', {binance_bid:Number(value('bid') || 0)});
    showPlan(p, false);
  } catch (error) {
    $('plan-state').textContent='配平需调整';
    $('plan-state').classList.add('warning');
    message('plan-result', error.message, true);
  }
}
$('check-binance').onclick = () => action('check-binance', async () => {
  await saveConfigSections(['binance','execution']);
  const {result:r}=await api('/api/binance/check',{
    api_key:$('trading-api-key').value.trim(),api_secret:$('trading-api-secret').value.trim()
  });
  const lines=[r.message,`${r.symbol} ${r.status||''} · ${r.base_asset}/${r.quote_asset} · ${r.transport}`,
    `Bid/Ask ${r.quote.bid} / ${r.quote.ask} · USDT/USD ${Number(r.usdt_usd.value).toFixed(6)}`];
  if(r.account) {
    lines.push(`账户状态：canTrade=${r.account.can_trade?'true':'false'} · ${r.account.asset_mode_label} · USDT 可用 ${Number(r.account.available).toFixed(2)} · USDT 钱包 ${Number(r.account.wallet).toFixed(2)}`);
    if(r.account.position_mode_label) lines.push(`持仓模式：${r.account.position_mode_label}（程序会按对应 positionSide 下单）`);
    if(r.account.asset_mode==='multi') lines.push('其他保证金资产余额与未实现盈亏已核验为 0；程序不会切换模式或划转资产。');
  }
  if(r.permissions) lines.push(`API 权限：enableFutures=${r.permissions.enable_futures?'true':'false'} · enableReading=${r.permissions.enable_reading?'true':'false'} · IP 白名单=${r.permissions.ip_restricted?'已限制':'未限制'}`);
  if(r.permission_warning) lines.push(`API 权限明细未能读取：${r.permission_warning}`);
  if(r.fees) lines.push(`账户费率：Maker ${Number(r.fees.maker).toFixed(4)}% / Taker ${Number(r.fees.taker).toFixed(4)}%`);
  if(r.transport_warning) lines.push('提示：'+r.transport_warning);
  lines.push('本次仅读检查，未发送任何订单。');
  $('bid').value=r.quote.bid;$('binance-ask').value=r.quote.ask;
  message('binance-result',lines.join('\n'),false);
});
$('check-public-ip').onclick = () => action('check-public-ip', async () => {
  await saveConfigSections(['binance']);
  const {result:r}=await api('/api/binance/public-ip',{});
  publicIpValue=r.ip;
  message('public-ip-result',`${r.ip} · ${r.route_label}。可填入币安 API 白名单；代理分流或出口变化后需重新检测。`,false);
  $('public-ip-result').classList.add('ok');
});
$('copy-public-ip').onclick = async () => {
  if(!publicIpValue) return;
  try {
    await navigator.clipboard.writeText(publicIpValue);
    message('public-ip-result',`${publicIpValue} · 已复制。请粘贴到币安 API 的 IP 白名单。`,false);
    $('public-ip-result').classList.add('ok');
  } catch (error) { message('public-ip-result','复制失败，请手动选择上方 IP。',true); }
};
$('check-mt5').onclick = () => action('check-mt5', async () => {
  hasResult = false;
  const existingAccount=value('account'), existingServer=value('mt5-server'), existingSymbol=value('mt5-symbol');
  const at = revision;
  // Persist the selected execution mode with the adapter. Otherwise changing
  // live/native to paper/paper would be validated against the stale mode.
  await saveConfigSections(['mt5','execution']);
  const {result} = await api('/api/mt5/check', {mcp_token:$('mt5-mcp-token').value.trim()});
  if (at !== revision) {
    message('mt5-result', '检查期间连接配置有修改，请重新检查当前配置。');
    return;
  }
  showResult(result);
  
  // 检查成功后自动应用结果
  if (result.connected && result.identity_matches && result.symbol) {
    await api('/api/mt5/apply', {});
    const mt5 = result.symbol;
    // 应用账户和服务器信息
    if (result.identity) {
      if (!existingAccount) $('account').value = result.identity.account;
      if (!existingServer) $('mt5-server').value = result.identity.server;
    }
    // 应用品种名称
    if (mt5.name && !existingSymbol) {
      $('mt5-symbol').value = mt5.name;
    }
    // 应用规格信息
    for (const [key,id] of [['contract_size_oz','contract'], ['volume_min','volume-min'], ['volume_step','volume-step']]) $(id).value = mt5[key];
    $('lots').step = mt5.volume_step; $('lots').min = mt5.volume_min; $('lots').max = mt5.volume_max;
    message('config-status', '已自动应用终端账户、服务器和黄金规格。仍未启用交易。');
    await calculatePlan(true);
  }
});
$('paper-reset').onclick = () => action('paper-reset', async () => {
  const {state:s} = await api('/api/paper/reset', {});
  message('plan-result', '纸面持仓已重置，共 ' + s.groups.length + ' 组。');
});
$('paper-toggle').onclick = () => action('paper-toggle', async () => {
  await saveConfig();
  const state = await api('/api/status');
  const currentState = state.state.running;
  
  if (currentState) {
    await api('/api/paper/stop', {});
    message('plan-result', '纸面引擎已停止；模拟持仓保留，可以继续编辑参数。');
  } else {
    const {message:m} = await api('/api/paper/start', {});
    message('plan-result', m);
  }
});
$('paper-step').onclick = () => action('paper-step', async () => {
  await saveConfig();
  const {action:a, metrics:m, state:s, closed:c} = await api('/api/paper/step', {
    binance_bid:Number(value('bid')), binance_ask:Number(value('binance-ask')),
    mt5_bid:Number(value('mt5-bid')), mt5_ask:Number(value('mt5-ask')),
  });
  const floating = s.groups.reduce((sum, g) => sum + g.gross_pnl_usdt, 0);
  const closed = c.length ? '；本次平仓毛利润 ' + c.reduce((sum, g) => sum + g.gross_pnl_usdt, 0).toFixed(2) + ' USDT' : '';
  const names = {hold:'等待', open_paper_group:'模拟开仓', close_paper_group:'模拟平仓'};
  message('plan-result', '纸面动作：' + (names[a] || a) + '；入场（卖币安 Bid − 买 MT5 Ask） ' + m.entry_spread.toFixed(3) + '；退出（买回币安 Ask − 卖 MT5 Bid） ' + m.exit_spread.toFixed(3) + '；浮动毛盈亏 ' + floating.toFixed(2) + ' USDT；持仓组 ' + s.groups.length + closed + '\n本模块尚未计入手续费、资金费率、隔夜费及返佣。');
});
for(const input of document.querySelectorAll('input:not([readonly]), select')) {
  const changed=()=>{
    if(input.id==='chart-window') return;
    revision++;
    if(SECRET_IDS.has(input.id)) {
      credentialsDirty=true; dirtySecrets.add(input.id);
      message('config-status','连接凭据已修改，等待保存到本机…'); queueAutoSave();
    } else {
      dirtyFields.add(input.id);
      // Normalize dependent fields before capturing the draft.
      if(['exec-mode','mt5-adapter'].includes(input.id)) {
        syncConditionalFields(input.id);
        dirtyFields.add('exec-mode'); dirtyFields.add('mt5-adapter');
      } else syncConditionalFields();
      saveDraft();
      message('config-status','有修改，等待保存到本机…'); queueAutoSave();
    }
    if(input.id==='binance-proxy') {
      publicIpValue=''; controls();
      $('public-ip-result').classList.remove('ok');
      message('public-ip-result','代理配置已修改，请重新检测币安出口 IP。');
    }
    if(Object.values(bindings.mt5).includes(input.id) || input.id==='mt5-mcp-token') {
      hasResult=false; controls();
      message('mt5-result','连接配置已修改，请重新检查 MT5；显示的规格为上次保存值。');
    }
    if(input.id==='lots' && hasResult) {
      clearTimeout(planTimer); planTimer=setTimeout(()=>calculatePlan(true),500);
    }
  };
  input.addEventListener('input',changed);
  input.addEventListener('change',changed);
}
let configLoading=false;
async function loadInitialConfig() {
  if(loaded || configLoading) return;
  configLoading=true;
  try {
    const {config}=await api('/api/config');
    const pending=draftFields(),draft=readDraft();
    let legacy=null;
    try { legacy=JSON.parse(localStorage.getItem(LEGACY_DRAFT_KEY)||'null')?.config; } catch(_) {}
    populate(legacy ? {...config,...legacy,mt5:{...config.mt5,...legacy.mt5}} : config);
    if(legacy) for(const input of document.querySelectorAll('input, select'))
      if(editableField(input.id)) dirtyFields.add(input.id);
    // Always load untouched saved fields, even if the user typed while GET
    // was in flight. Current edits win over the draft and the saved config.
    restoreFields(draft?.fields); restoreFields(pending);
    syncConditionalFields(); loaded=true; controls();
    if(dirtyFields.size) {
      revision++; saveDraft(); queueAutoSave();
      message('config-status','已恢复未保存的输入，正在继续保存；其余字段已读取本机配置。');
    } else message('config-status','已读取本机配置；修改后自动保存，凭据也会保存在本机。');
    window.dispatchEvent(new Event('gold-config-ready'));
  } catch(error) { message('config-status','读取本机配置失败：'+error.message+'；暂不提交表单，稍后自动重试。',true); }
  finally { configLoading=false; }
}
async function refresh() {
  if (pollBusy) return;
  pollBusy = true;
  try {
    const j = await api('/api/status');
    message('connection', '本机应用已连接');
    message('state', '服务 ' + j.host + ':' + j.port + ' · 旧版纸面引擎' + (j.state.running ? '已启动（需手动推进）' : '已停止') + '；自动执行状态见下方。');
    
    // 更新纸面引擎按钮状态
    const paperToggleButton = $('paper-toggle');
    if (j.state.running) {
      paperToggleButton.textContent = '停止纸面引擎';
      paperToggleButton.classList.remove('primary');
    } else {
      paperToggleButton.textContent = '启动纸面引擎';
      paperToggleButton.classList.add('primary');
    }
    
  } catch (error) { message('connection','本机服务未连接',true); }
  finally { pollBusy = false; }
}
loadInitialConfig(); refresh(); setInterval(()=>{loadInitialConfig();refresh();},5000);
window.addEventListener('pagehide',saveDraft);
window.addEventListener('beforeunload',saveDraft);
