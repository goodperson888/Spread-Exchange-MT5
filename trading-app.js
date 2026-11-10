'use strict';
(() => {
  const el = id => document.getElementById(id);
  const text = (id, value, error=false) => {
    el(id).textContent=value; el(id).classList.toggle('error',error);
    if(id==='trading-result'&&el('records-feedback')) {
      el('records-feedback').textContent=value;el('records-feedback').classList.toggle('error',error);
    }
  };
  const unconfirmed = (g,orders) => orders.filter(o=>o.group===g.id&&o.result?.status!=='done');
  const expandedRules=new Set();
  const valuationPending=(g,orders)=>g.status!=='closed'&&(!last?.connected||g.valuation_status==='unverified'||g.status!=='open'||g.external_attention||unconfirmed(g,orders).length>0||last?.state?.recovery||last.capabilities?.mode==='live'&&!last.capabilities.reconciled);
  const n = x => Number(x).toLocaleString('zh-CN',{maximumFractionDigits:3});
  const date = t => t ? new Date(t).toLocaleTimeString() : '—';
  const dateTime = t => t ? new Date(t).toLocaleString('zh-CN') : '—';
  const usd = x => Number.isFinite(Number(x)) ? `${Number(x)>=0?'+':''}${Number(x).toFixed(2)} USD` : '—';
  const escape = value => String(value ?? '').replace(/[&<>'"]/g, x => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[x]));
  const eventLabels={boot:'程序启动',start:'启动自动开仓',pause:'暂停自动开仓',entry_preflight_blocked:'开仓预检暂未通过',entry_skipped:'跳过本次开仓',group_opening:'准备开仓',group_opened:'双边开仓完成',group_closed:'交易组已结束',order_intent:'准备发送订单',order_result:'订单回报',order_reconciled:'订单对账完成',entry_recovered:'延迟回报已确认，双边开仓完成',order_reconcile_retry:'订单对账重试',order_reconcile_exhausted:'快速确认结束，继续低频对账',close_requested:'请求平仓',close_attempt:'尝试平仓',exit_signal:'触发退出条件',entry_threshold_applied:'开仓阈值已更新',reconciled:'持仓对账通过',paper_reconciled:'纸面持仓对账通过',auto_reconciled_flat:'空仓自动对账通过',costs_verified:'费用已核验',cost_verification_pending:'等待费用核验',exit_rules_updated:'已有组退出规则已修改',external_orders_resolved:'原订单成交已核对',recovery_flat_confirmed:'已确认旧组双边平仓',external_position_changed:'发现平台持仓变化，已暂停交易',external_close_backfilled:'平台外部平仓记录已回补',adoption_management:'已有仓位管理状态变更',adoption_closing_costs_checked:'接管仓位平仓费用已核验'};
  const eventFields={request_ms:'下单回报耗时（毫秒）',query_ms:'订单查询耗时（毫秒）',query_timeout_ms:'查询超时预算（毫秒）',error_kind:'错误分类',reason_code:'暂停原因代码',reason_label:'暂停原因',time_ms:'记录时间（毫秒）',enabled_before:'暂停前开仓已启用',keep_entry:'保留原确认时限',threshold:'开仓阈值',reason:'原因',state:'状态',group:'交易组',id:'订单标识',leg:'平台',action:'操作',qty:'数量',price:'价格',status:'回报状态',error:'说明',result:'回报',mode:'模式',entry:'开仓价差',value:'数值',attempt:'尝试次数',net:'净收益',net_estimate:'预估净收益',signal_spread:'触发价差',binance_qty:'币安数量',mt5_lots:'MT5手数',position_mode:'持仓模式',enabled:'启用',code:'原因代码',ticket:'订单号',submitted:'已发送',raw_status:'交易所状态',error_code:'错误码',fee:'手续费',changes:'修改项',before:'原规则',after:'新规则',revision:'规则版本',target_mode:'止盈方式',take_contraction_usd:'收窄量',exit_spread_usd:'退出价差',require_net_profit:'要求最低净收益',min_net_profit_usd:'最低净收益',group_loss_enabled:'单组亏损限制启用',group_max_loss_usd:'单组最大亏损',max_hold_minutes:'从原开仓计持仓时限（分钟）'};
  const eventValues={waiting:'等待检查',checking:'检查中',expired:'已过期',failed:'失败',ready:'通过',done:'已结束',pending:'待确认',unknown:'未知',binance:'币安',mt5:'MT5',open:'开仓',close:'平仓',paper:'纸面',live:'实盘',hedge:'双向持仓',one_way:'单向持仓',contraction:'价差收窄',absolute:'绝对退出价差'};
  function eventDetails(value,key='') {
    if(value&&typeof value==='object') return Object.entries(value).map(([k,v])=>`${eventFields[k]||k}：${eventDetails(v,k)}`).join('；');
    if(typeof value==='boolean')return value?'是':'否';
    return ['state','status','leg','action','mode','position_mode','target_mode'].includes(key)?(eventValues[value]||String(value??'—')):String(value??'—');
  }
  function renderEvents(events) {
    const rows=[];
    for(const event of events.slice(0,100)) {
      const previous=rows[rows.length-1], signature=event.kind+JSON.stringify(event.data||{});
      if(previous&&previous.signature===signature&&Math.abs(previous.time-event.time)<15000) {previous.count++;continue;}
      rows.push({...event,signature,count:1});
    }
    const content=rows.map(x=>{
      const data=x.data||{};
      const detail=x.kind==='entry_preflight_blocked'
        ?`开仓阈值 ${data.threshold??'—'} USD/盎司；${data.reason||'旧日志未记录具体原因，请查看开仓阈值下方的预检状态'}；本次未下单，等待预检通过`
        :eventDetails(data);
      return `${dateTime(x.time)}  ${eventLabels[x.kind]||'技术事件（'+x.kind+'）'}${x.count>1?'（合并 '+x.count+' 条）':''}\n${detail}`;
    }).join('\n\n')||'暂无交易事件';
    const node=el('trade-events');
    if(node.textContent!==content) {
      const offset=node.scrollTop;node.textContent=content;node.scrollTop=offset;
    }
  }
  let chart, timer, busy=false, pauseBusy=false, last=null, resetChartZoom=true, actionError='', reconcileError=false;
  let chartView=null, stream=null, streamReady=false, paintPending=false, lastPaintAt=0, chartSamples=[], plotGeneration=0, latestLive=null, statusBusy=false;
  let pendingQuotes=[], paintTimer=null, paintFrame=null, plotController=null;
  let historyDirty=true, historyKey=null, historyLoadedAt=0, historyRevision=0;
  function invalidateHistory(){historyDirty=true;historyRevision++;}
  // Keep the browser-side high-frequency window bounded; longer history comes
  // from the local SQLite chart endpoint and is already downsampled for display.
  const quoteBuffer=window.GoldPairQuotes?new window.GoldPairQuotes.ChartBuffer(30000,20000):null;
  function windowStart(){return Date.now()-Math.max(1,Number(el('chart-window').value)||1440)*60000;}
  function paintSoon(){
    if(el('view-trading')?.hidden) {
      if(el('view-records')?.hidden===false)renderIncome(latestLive||last?.quote);
      return;
    }
    if(document.hidden||paintPending)return;paintPending=true;
    const run=()=>{
      paintPending=false;paintTimer=null;paintFrame=null;
      if(document.hidden)return;
      if(pendingQuotes.length){
        chartSamples=quoteBuffer.merge(pendingQuotes,windowStart(),last?.quote?.key);
        pendingQuotes=[];
      }
      lastPaintAt=Date.now();drawChart(chartSamples);renderIncome(latestLive||last?.quote);
    };
    const delay=Math.max(0,200-(Date.now()-lastPaintAt));
    if(delay) paintTimer=setTimeout(run,delay);
    else if(window.requestAnimationFrame) paintFrame=window.requestAnimationFrame(run);
    else paintTimer=setTimeout(run,16);
  }
  function streamLabel(){
    const badge=el('chart-live-status');if(!badge)return;
    const q=latestLive||last?.quote;
    const stale=q&&(q.valid===false||Date.now()-Math.min(q.mt5?.observed_ms||q.mt5?.time_ms||0,q.binance?.observed_ms||q.binance?.time_ms||0)>10000);
    badge.textContent=!last?.connected?'未连接 · 图表保留历史':stale?'行情过期或双边不同步':streamReady?'图表长连接 · 收到报价即更新':'图表重连中 · 历史保留';
    badge.classList.toggle('warning',!streamReady||stale||!last?.connected);
  }
  function startStream(){
    if(document.hidden||!window.EventSource||stream)return;
    stream=new window.EventSource('/api/trading/stream');
    const source=stream;
    stream.onopen=()=>{streamReady=true;streamLabel();};
    stream.onerror=()=>{streamReady=false;streamLabel();};
    stream.addEventListener('quotes',event=>{
      if(document.hidden||source!==stream)return;
      try {
        const payload=JSON.parse(event.data),key=last?.quote?.key;
        const rows=(payload.samples||[]).filter(q=>!key||q.key===key);
        if(rows.length){
          if(latestLive&&rows[0].time_ms-latestLive.time_ms>10000)invalidateHistory();
          latestLive=rows.at(-1);
          for(const row of rows)pendingQuotes.push(row);
          if(pendingQuotes.length>30000)pendingQuotes=pendingQuotes.slice(-30000);
          const q=latestLive,age=x=>Math.max(0,Date.now()-(x?.observed_ms||x?.time_ms||0)),sourceTime=x=>x?.source_time_ms||x?.time_ms||0;
          const sourceGap=Math.abs(sourceTime(q.mt5)-sourceTime(q.binance));
          el('quote-latency').textContent=`MT5 ${q.mt5_transport||'行情'} · 币安 ${q.binance_transport||'行情'} · 报价年龄：MT5 ${age(q.mt5)} ms / 币安 ${age(q.binance)} ms · 本机收到时间差 ${Math.abs((q.mt5?.observed_ms||0)-(q.binance?.observed_ms||0))} ms · 原始时间差 ${sourceGap>=60000?'约 '+n(sourceGap/60000)+' 分钟（不代表传输延迟）':sourceGap+' ms'}${q.valid===false?' · 当前报价不满足交易校验':''}`;
          showQuote(q);paintSoon();
        }
        streamLabel();
        if(payload.reset)invalidateHistory();
        if(historyDirty&&!plotController&&!el('view-trading')?.hidden&&(payload.reset||Date.now()-historyLoadedAt>15000))plot().catch(error=>text('trading-result','恢复历史失败：'+error.message,true));
      } catch(error){text('trading-result','行情推送解析失败：'+error.message,true);}
    });
  }
  let adoptionPreview=null, adoptionReportTime=null, adoptionGroupId=null, adoptionRevision=0;

  async function request(path, body, controller=new AbortController()) {
    const timeout=setTimeout(()=>controller.abort(),30000);
    try {
      const response = await fetch(path, {signal:controller.signal,method:body === undefined?'GET':'POST',headers:{'Content-Type':'application/json','X-Local-App':'GoldPairLocal'},body:body===undefined?undefined:JSON.stringify(body),cache:'no-store'});
      const result = await response.json();
      if (!response.ok || result.ok===false) throw Error((result.errors || [result.error || '请求失败']).join('；'));
      return result;
    } catch(error) {
      if(error.name==='AbortError') throw Error('请求超过 30 秒，请检查网络或 MT5 终端后重试。');
      throw error;
    } finally { clearTimeout(timeout); }
  }
  function lock(value, activeId, label) {
    busy=value;
    for(const id of ['trading-connect','trading-reconcile','trading-toggle','trading-close','apply-entry','adoption-read','adoption-preview','adoption-confirm','adoption-manage','exit-rule-apply']) {
      el(id).disabled=value;
      el(id).classList.toggle('processing', value);
    }
    if(!value) el('adoption-confirm').disabled=!adoptionPreview;
    const active=activeId&&el(activeId);
    if(active?.tagName==='BUTTON') {
      if(value) { active.dataset.label=active.textContent; active.dataset.pendingLabel=label||'处理中…'; active.textContent=active.dataset.pendingLabel; }
      else if(active.textContent===active.dataset.pendingLabel) active.textContent=active.dataset.label||active.textContent;
    }
  }
  async function action(work, activeId, label) {
    const urgent=activeId==='trading-close'||activeId==='group-close';
    if(busy||window.goldPairUiBusy&&!urgent)return; actionError='';reconcileError=false;
    lock(true,activeId,label);
    if(typeof setUiBusy==='function')setUiBusy(1);else window.goldPairUiBusy=true;
    text('trading-result', label||'正在处理，请稍候…');
    const adoptionAction=activeId?.startsWith('adoption-');
    if(adoptionAction)text('adoption-feedback',label||'正在处理…');
    try {
      await work();
      if(adoptionAction)text('adoption-feedback',activeId==='adoption-confirm'?'已登记接管；旧仓管理尚未启动，本次没有下单。':'操作完成，请查看下方预览或管理状态。');
    } catch(error) {
      reconcileError=activeId==='trading-reconcile';actionError='本次操作未完成：'+error.message; text('trading-result',actionError,true);
      if(adoptionAction)text('adoption-feedback',actionError,true);
      if(activeId==='exit-rule-apply'){exitPreview=null;text('exit-rule-result',actionError,true);}
    }
    finally {
      if(typeof setUiBusy==='function')setUiBusy(-1);else window.goldPairUiBusy=false;
      lock(false,activeId);updateWorkflow();renderExitEditor();
    }
  }
  function groupRows(groups, orders) {
    if(!groups?.length) return '<p class="note">暂无本策略持仓或交易记录。</p>';
    const states={opening:'开仓中',open:'持仓中',closing:'平仓中',unwinding:'异常撤回',attention:'需人工处理',closed:'已平仓'};
    const rows=groups.slice().reverse().map(g => {
      const v=g.valuation||{}, remaining=v.remaining||{};
      const pending=unconfirmed(g,orders), unknownLegs=new Set(pending.map(o=>o.leg)), unverified=valuationPending(g,orders);
      const remainingLabel=leg=>unverified?`日志 ${n(remaining[leg]||0)}（实际待核对）`:unknownLegs.has(leg)?`待确认（已确认 ${n(remaining[leg]||0)}）`:n(remaining[leg]||0);
      const own=orders.filter(o=>o.group===g.id&&o.action==='open'&&Number(o.result?.qty)>0);
      const amount=leg=>own.filter(o=>o.leg===leg).reduce((sum,o)=>sum+Number(o.result.qty)*Number(o.result.price),0);
      const basis=g.imported?'接管旧仓 · 成本估算':g.mode==='paper'?'纸面估算':v.costs_verified?'平台已复核':'实盘估算';
      const funding=Number(v.binance_funding||0),swap=Number(v.mt5_swap||0);
      const carryDetail=('binance_funding' in v||'mt5_swap' in v)?`资金费 ${usd(funding)}<br>MT5 Swap ${usd(swap)}`:`合计 ${usd(v.carry||0)}`;
      const abnormalClosed=g.status==='closed'&&/拒单|异常|敞口|部分/.test(String(g.reason||''));
      const action=g.status==='closed'?'':g.status==='closing'?`<button disabled>${pending.length?'核对成交后继续平仓':'平仓处理中…'}</button>`:`<button data-close="${escape(g.id)}"${busy?' disabled':''}>${pending.length?'请求核对后平仓':g.status==='attention'?'重试平仓':'平仓'}</button>`;
      const editAction=g.status==='open'&&!g.imported&&!pending.length?`<button data-edit-exit="${escape(g.id)}" type="button"${busy?' disabled':''}>调整退出条件与返佣</button>`:'';
      const entryLabel=g.open_binance && Number.isFinite(Number(g.entry))?`<br><span class="muted" title="币安开仓成交价 × 该笔记录的 USDT/USD − MT5 开仓成交价；双边成交可能不在同一时刻">开仓成交价差 ${n(g.entry)} USD/盎司</span>`:'';
      const gridLabel=Number(g.grid_index||0)>0?`<br><span class="muted">网格补仓第 ${Number(g.grid_index)} 次</span>`:'';
      const exitFee=Number(v.estimated_exit_fee||0), exitBinance=Number(v.estimated_exit_fee_binance||0), exitMt5=Number(v.estimated_exit_fee_mt5||0);
      const feeDetail=unverified?'<br><span class="muted">预计平仓费待持仓核对</span>':exitFee>0?`<br><span class="muted" title="按当前币安 Ask × USDT/USD × 币安 taker 费率，加上 MT5 剩余手数 × 单边每手佣金估算">预计平仓费 -${exitFee.toFixed(2)} USD<br>币安 -${exitBinance.toFixed(2)} + MT5 -${exitMt5.toFixed(2)}</span>`:'';
      const rebate=Number(v.estimated_rebate||0),hasRebate=rebate>0||Number(g.costs?.binance_rebate_percent)>0||Number(g.costs?.mt5_rebate_percent)>0;
      const rebateDetail=hasRebate?`<br><span class="muted" title="按填写比例估计，含预计平仓费返佣；尚未核对到账，不覆盖原手续费">预计返佣 ${unverified?'待核对':usd(rebate)}（未确认到账）${unverified?'':'<br>未含返佣净收益 '+usd(v.net_before_rebate)}</span>`:'';
      const stateLabel=pending.length?'成交待确认 · '+(states[g.status]||g.status):abnormalClosed?'异常撤回已完成':states[g.status]||g.status;
      const pnlLabel=unverified?'收益待核对':g.status==='closed'?(abnormalClosed?'异常撤回净结果':'平仓净收益')+(g.mode==='paper'?'（纸面模拟）':g.imported?'（含接管历史估算）':v.costs_verified?'':'（估算／待费用复核）'):'实时净收益（估算）';
      const feeLabel=g.mode==='paper'?'模拟成交手续费':v.costs_verified?'已核验成交手续费':'成交手续费（含估算）';
      const feeTitle=g.mode==='paper'?'按纸面配置模拟，不读取实盘成交费用':v.costs_verified?'已用平台成交记录复核':'已发生成交可能仍按费率或每手佣金估算；普通实盘组平仓后异步复核，接管前成本为用户录入';
      const s=g.parameters||{},ruleNumber=value=>Number.isFinite(value)?n(value):'—',ruleLabel=s.target_mode==='absolute'?`退出价差 ≤ ${ruleNumber(s.exit_spread_usd)}`:`收窄 ≥ ${ruleNumber(s.take_contraction_usd)}`;
      const savedRules=`<details class="help-details" data-group-rules="${escape(g.id)}"${expandedRules.has(g.id)?' open':''}><summary>${g.imported?'本组当前规则':'本组已锁定规则'}</summary><p class="note">止盈：${ruleLabel} USD/盎司${s.require_net_profit?'；净收益至少 '+usd(s.min_net_profit_usd):''}<br>单组亏损限制：${s.group_loss_enabled?usd(s.group_max_loss_usd):'未启用'}<br>持仓时限：${Number(s.max_hold_minutes)>0?n(s.max_hold_minutes)+' 分钟':'未启用'}<br>预计手续费返佣：币安 ${n(g.costs?.binance_rebate_percent||0)}% / MT5 ${n(g.costs?.mt5_rebate_percent||0)}%（计入净收益，非到账）<br>普通 / 保护滑点：${ruleNumber(s.max_slippage_usd)} / ${ruleNumber(s.unwind_slippage_usd)} USD/盎司<br>主页面改值不会覆盖本组规则；普通组可在下方退出条件入口单独预览确认。全策略总亏损限制按当前引擎参数执行。${g.imported?'接管组退出规则可在旧仓管理区单独修改。':''}</p></details>`;
      return `<tr><td><strong>#${escape(g.id)}</strong>${gridLabel}<br><span class="muted">${dateTime(g.opened_ms)}${g.closed_ms?'<br>→ '+dateTime(g.closed_ms):''}</span></td><td>${escape(stateLabel)}<br><span class="record-basis">${basis}</span>${g.execution_warning?'<br><span class="muted">'+escape(g.execution_warning)+'</span>':''}</td><td>${n(g.lots)} 手 / ${n(g.qty)} 盎司${entryLabel}<br><span class="muted">币安 ${amount('binance').toFixed(2)} USDT<br>MT5 ${amount('mt5').toFixed(2)} USD</span></td><td>毛收益 ${unverified?'待核对':usd(v.gross)}<br><span title="${feeTitle}">${feeLabel} ${usd(-Number(v.fees||0))}</span>${feeDetail}${rebateDetail}</td><td>${carryDetail}</td><td class="${Number(v.net)>=0?'pnl-positive':'pnl-negative'}"><strong>${unverified?'—':usd(v.net)}</strong><br><span class="muted">${pnlLabel}${hasRebate&&!unverified?' · 含预计返佣':''}</span></td><td>币安 ${remainingLabel('binance')}<br>MT5 ${remainingLabel('mt5')}${g.reason?'<br>'+escape(g.reason):''}</td><td>${action}${editAction}${savedRules}</td></tr>`;
    }).join('');
    return `<table class="records-table"><thead><tr><th>交易组 / 时间</th><th>状态</th><th>配平量 / 开仓金额</th><th>交易收益与手续费</th><th>资金费 / Swap</th><th>净收益</th><th>剩余敞口 / 原因</th><th>操作</th></tr></thead><tbody>${rows}</tbody></table>`;
  }
  function orderRows(orders,groups) {
    if(!orders?.length) return '<p class="note">暂无双边订单。每次下单意图会先持久化，再发送到平台。</p>';
    const byGroup=new Map(groups.map(g=>[g.id,g]));
    const rows=orders.slice().reverse().slice(0,100).map(o=>{
      const r=o.result||{},g=byGroup.get(o.group),filled=Number(r.qty||0),price=Number(r.price||0);
      const direction=o.leg==='binance'?(o.action==='open'?'卖出':'买入'):(o.action==='open'?'买入':'卖出');
      const quantity=o.leg==='mt5'&&g?`${n(filled)} 盎司 / ${n(filled/g.contract)} 手`:`${n(filled)} XAU`;
      const status=o.source==='external'?'平台外部平仓 · 已回补':o.imported?'原持仓成本登记（本次未下单）':r.status==='done'?(filled>0?'已成交':'未成交'):(r.status==='pending'?'待确认':'状态未知');
      const fee=o.imported?'历史费用见接管汇总':Number.isFinite(Number(r.fee))?`${Number(r.fee).toFixed(4)} ${o.leg==='binance'?'USDT':'USD'}`:'未单独回填';
      const stages=[];
      if(Number.isFinite(o.dispatch_ms)&&Number.isFinite(o.signal_time_ms)) stages.push(`采样→调用 ${Math.max(0,o.dispatch_ms-o.signal_time_ms)} ms`);
      if(Number.isFinite(o.intent_journal_ms)) stages.push(`意图落盘 ${o.intent_journal_ms} ms`);
      if(Number.isFinite(r.request_ms)) stages.push(`适配器调用 ${r.request_ms} ms`);
      if(o.source!=='external'&&Number.isFinite(r.reconcile_time_ms)) stages.push(`查询确认 ${Math.max(0,r.reconcile_time_ms-o.created_ms)} ms（非实际成交延迟）`);
      else if(!Number.isFinite(r.request_ms)&&Number.isFinite(r.fill_time_ms)) stages.push(`意图→本机回报 ${Math.max(0,r.fill_time_ms-o.created_ms)} ms`);
      const timing=o.source==='external'?'<br><span class="muted">平台成交时间 · 后台回补</span>':o.imported||!stages.length?'':`<br><span class="muted" title="本机测量，含适配器内部校验；不是交易所撮合时间">${stages.join('<br>')}</span>`;
      const signal=Number.isFinite(o.signal_spread)?n(o.signal_spread):'—';
      const actual=Number.isFinite(o.actual_spread)?n(o.actual_spread):'等待双边成交';
      const spreadDelta=Number.isFinite(o.spread_slippage)?`${o.spread_slippage>=0?'+':''}${n(o.spread_slippage)}`:'—';
      return `<tr><td>${dateTime(o.created_ms)}${timing}</td><td>#${escape(o.group)}</td><td>${o.leg==='binance'?'币安':'MT5'}<br>${escape(o.symbol)}</td><td>${o.action==='open'?'开仓':'平仓'} · ${direction}</td><td>${o.source==='external'?'外部成交':'申请'} ${n(o.requested)} 盎司<br>${r.status==='done'?'成交 '+quantity:'成交待确认（已确认 '+quantity+'）'}</td><td>${price?`${n(price)}<br>${(filled*price).toFixed(2)} ${o.leg==='binance'?'USDT':'USD'}`:'—'}</td><td>${signal}<br><span class="muted">${actual}</span><br><span class="muted">偏移 ${spreadDelta}</span></td><td>${fee}</td><td>${status}${r.error?'<br>'+escape(r.error):''}<br><span class="muted">${escape(r.ticket||o.id)}</span></td></tr>`;
    }).join('');
    return `<table class="records-table"><thead><tr><th>时间</th><th>交易组</th><th>平台 / 品种</th><th>动作</th><th>申请 / 成交数量</th><th>成交价 / 金额</th><th>监控价差<br>实际成交价差<br>偏移</th><th>成交手续费</th><th>状态 / 票据</th></tr></thead><tbody>${rows}</tbody></table>`;
  }
  function showQuote(q) {
    if(q) {
      el('quote-summary').textContent=`币安 Bid/Ask ${n(q.binance.bid)} / ${n(q.binance.ask)}（原始 ${date(q.binance.source_time_ms||q.binance.time_ms)}） · MT5 Bid/Ask ${n(q.mt5.bid)} / ${n(q.mt5.ask)}（原始 ${date(q.mt5.source_time_ms||q.mt5.time_ms)}） · 入场（卖币安 Bid − 买 MT5 Ask） ${n(q.entry)} · 退出（买回币安 Ask − 卖 MT5 Bid） ${n(q.exit)} USD/盎司`;
      el('bid').value=q.binance.bid;el('binance-ask').value=q.binance.ask;
      el('mt5-bid').value=q.mt5.bid;el('mt5-ask').value=q.mt5.ask;
      text('live-entry',n(q.entry));text('live-exit',n(q.exit));
      const threshold=last?.strategy_runtime?.entry_spread_usd;
      const valid=q.valid!==false&&Date.now()-Math.min(q.mt5.observed_ms||q.mt5.time_ms,q.binance.observed_ms||q.binance.time_ms)<=Number(last?.strategy_runtime?.max_quote_age_ms||3000);
      text('live-distance',Number.isFinite(threshold)?n(Math.max(0,threshold-q.entry))+' USD':'—');
      text('live-opportunity',!valid?'报价过期或不同步，禁止开仓':!Number.isFinite(threshold)?'连接后显示有效阈值':q.entry>=threshold?'达到基础阈值，仍须通过执行保护':'等待价差达到基础阈值');
    }
  }
  function updateEntryLabel() {
    const effective=el('entry-effective'), value=effective.dataset.value;
    const pending=el('entry').value.trim();
    effective.textContent=value!==undefined&&value!==''
      ? `当前生效：${n(Number(value))}`+(pending!==''&&Number(pending)===Number(value)?'（已有交易组不变）':` · 待应用：${pending||'未填写'}`)
      : '当前生效：未连接';
  }
  function updateWorkflow() {
    const state=last?.state||{}, connected=Boolean(last?.connected), reconciled=last?.capabilities?.reconciled===true;
    const blocked=(state.groups||[]).some(g=>['opening','unwinding','closing','attention'].includes(g.status))||(state.orders||[]).some(o=>o.result?.status!=='done');
    const pending=connected&&(last?.config_pending===true || typeof dirtyFields!=='undefined'&&[...dirtyFields].some(id=>!id.startsWith('risk-')));
    if(!connected)el('trading-mode').textContent=(el('exec-mode').value==='live'?'实盘配置':'纸面配置')+' · 未连接';
    const status=el('workflow-status');
    if(status)status.textContent=!connected?'下一步：连接双边行情（首次使用先完成连接设置）':pending?'参数有变更：当前引擎继续使用连接时配置；暂停新开仓后重新连接生效。':!reconciled?'下一步：持仓对账，通过后可启动自动开仓。':state.recovery?'等待持仓对账，通过前不能启动自动开仓':blocked?'成交或平仓尚未完成，暂不能启动自动开仓':state.enabled?'自动开仓运行中 · 暂停只停止新开仓，已有仓位继续管理。':'准备就绪 · 可启动自动开仓';
    if(!busy) {
      el('trading-connect').disabled=Boolean(state.enabled||window.goldPairUiBusy);
      el('trading-toggle').disabled=Boolean(state.enabled||pauseBusy||!connected||!reconciled||state.recovery||blocked||pending||window.goldPairUiBusy);
      el('trading-reconcile').disabled=!connected||window.goldPairUiBusy;
      el('trading-close').disabled=!connected||!(state.groups||[]).some(g=>g.status!=='closed');
      el('apply-entry').disabled=!connected;
      for(const button of el('groups').querySelectorAll('[data-close]'))button.disabled=Boolean(window.goldPairUiBusy);
    }
    el('trading-pause').disabled=pauseBusy||!connected||!state.enabled;
    el('trading-pause').textContent=pauseBusy?'正在暂停新开仓…':'暂停新开仓';
    text('new-entry-status',pauseBusy?'新开仓：正在暂停，等待服务端确认':!connected?'新开仓：未连接':state.enabled?'新开仓：运行中':state.recovery||connected&&!reconciled?'新开仓：已暂停 · 等待持仓核对，已有仓位管理暂未恢复':'新开仓：已暂停 · 已有仓位继续按规则管理');
    el('new-entry-status').dataset.state=pauseBusy?'pending':state.enabled?'running':'paused';
    const pendingNode=el('config-pending');
    if(pendingNode){pendingNode.hidden=!pending;pendingNode.textContent='已保存 ≠ 已生效。手数、止盈等交易参数须重新连接；开仓阈值可单独点击「应用」，风险提醒阈值保存即生效。已有组保留原规则。';}
    const groups=(state.groups||[]).filter(g=>g.status!=='closed');
    text('parameter-scope',pending?'参数已修改，尚未全部生效；重新连接后用于后续开仓，现有组保留原规则；修改已有组请到记录页单独预览确认。':connected?`现有 ${groups.length} 组保留各自开仓规则；在记录页可查看。开仓阈值须点“应用”，风险提醒保存即生效。`:'手数和普通策略参数须保存并连接后生效；已有组不会随输入改变。');
    text('live-groups',groups.length+' 组');
    const unknown=groups.some(g=>valuationPending(g,state.orders||[]));
    text('live-exposure',unknown?'实际持仓待核对；日志余额不代表平台剩余仓位':groups.length?'MT5 '+n(groups.reduce((sum,g)=>sum+Number(g.valuation?.remaining?.mt5||0)/Number(g.contract||100),0))+' 手（已确认剩余）':connected?'当前无本策略持仓':'等待连接与对账');
  }
  function renderPositionFreshness() {
    if(!last)return;
    const state=last.state||{},report=last.position_report;
    const q=latestLive?.key===last.quote?.key?latestLive:last.quote;
    const stale=!q||q.valid===false||['mt5','binance'].some(side=>Date.now()-Number(q[side]?.observed_ms||q[side]?.time_ms||0)>Number(last.strategy_runtime?.max_quote_age_ms||3000));
    const age=Number.isFinite(report?.time_ms)?Math.max(0,Math.floor((Date.now()-report.time_ms)/1000)):null;
    let value=!last.connected?'未连接：已有仓位未恢复自动管理。':last.capabilities?.mode==='paper'?'纸面模式：收益随模拟行情估算，不读取实盘持仓。':
      (state.orders||[]).some(o=>o.result?.status!=='done')?'成交待确认：剩余数量与收益等待核对。':
      state.recovery||!last.capabilities?.reconciled?'等待完整持仓对账：报价估值不代表平台仓位已核实。':
      `${stale?'报价已过期，收益仅按最后收到的报价估算。':'收益随报价估算；'}${age!==null?'上次完整对账 '+dateTime(report.time_ms)+'（'+age+' 秒前）。':'尚无完整对账时间记录。'}完整对账非持续进行；后台仅按成交历史回补可归属的外部平仓。`;
    const monitor=last.external_close_status;
    if(last.connected&&last.capabilities?.mode==='live'&&monitor) {
      const active=(state.groups||[]).some(g=>g.status!=='closed');
      const outdated=active&&Date.now()-Number(monitor.time_ms||0)>20000;
      value+=' 后台核对：'+(outdated?'数据已过期，请检查连接或点击持仓对账。':monitor.message);
    }
    text('position-check-status',value);text('records-position-check',value);
  }
  let riskSample=null, riskReceivedAt=0;
  function renderRisk() {
    const elapsed=Math.max(0,Date.now()-riskReceivedAt);
    const fmt=(value,suffix='')=>typeof value==='number'&&Number.isFinite(value)?n(value)+suffix:'—';
    const metric=(label,value)=>`<div><span>${escape(label)}</span><strong>${escape(value)}</strong></div>`;
    const labels={normal:'未触发预警',warning:'接近预警线',danger:'危险提醒',unknown:'风险状态未知',stale:'数据过期',paper:'纸面模式',disconnected:'未连接'};
    for(const side of ['binance','mt5']) {
      const card=el(`risk-${side}-card`);if(!card)continue;
      const sample=riskSample?.[side]||{level:'unknown',message:'等待账户风险数据'};
      const age=typeof sample.age_ms==='number'?sample.age_ms+elapsed:null;
      const stale=age!==null&&age>=(sample.stale_after_ms??20000)&&!['paper','disconnected'].includes(sample.level);
      const known=Object.prototype.hasOwnProperty.call(labels,sample.level)||sample.level==='idle';
      const level=stale?'stale':known?sample.level:'unknown';
      card.dataset.level=level;
      el(`risk-${side}-state`).textContent=level==='idle'?(side==='binance'?'无该品种持仓':'无保证金占用'):labels[level];
      el(`risk-${side}-message`).textContent=stale?'风险数据已过期，当前风险状态未知；以下数值为上次快照。':sample.message||'风险状态未知';
      el(`risk-${side}-age`).textContent=age===null?'尚无真实账户快照':`账户快照 ${Math.floor(age/1000)} 秒前${sample.updated_ms?' · '+dateTime(sample.updated_ms):''}`;
      let metrics='';
      if(side==='binance') {
        metrics=metric('最近强平距离',fmt(sample.distance_percent,'%'))+
          metric('账户保证金余额',fmt(sample.equity,' USDT'))+
          metric('账户维持保证金',fmt(sample.maintenance_margin,' USDT'))+
          metric('账户可用余额',fmt(sample.available,' USDT'));
        metrics+=(sample.positions||[]).map(p=>`<div class="risk-position">${p.side==='short'?'空头':'多头'} ${escape(fmt(p.qty))} XAU · 标记价 ${escape(fmt(p.mark_price))} · 强平价 ${escape(p.liquidation_price>0?fmt(p.liquidation_price):'未返回有效值')} USDT · 距离 ${escape(fmt(p.distance_percent,'%'))}</div>`).join('');
      } else {
        const currency=sample.currency||'账户币种';
        const unit=sample.margin_so_mode===0?'%':sample.margin_so_mode===1?' '+currency:'';
        metrics=metric('账户保证金水平',sample.margin>0?fmt(sample.margin_level,'%'):'—')+
          metric('经纪商强平线',fmt(sample.margin_so_so,unit))+
          metric('经纪商追保线',fmt(sample.margin_so_call,unit))+
          metric('账户净值 / 可用保证金',`${fmt(sample.equity)} / ${fmt(sample.free_margin)} ${currency}`);
        if(sample.margin_so_mode===1)metrics+='<div class="risk-position">金额强平模式 · 不换算黄金强平价</div>';
      }
      el(`risk-${side}-metrics`).innerHTML=metrics;
    }
  }
  function render(result) {
    riskSample=result.liquidation_risk||null;riskReceivedAt=Date.now();renderRisk();
    last=result;
    const incomingKey=result.quote?.key;
    if(incomingKey&&incomingKey!==historyKey){
      historyKey=incomingKey;invalidateHistory();
      chartSamples=quoteBuffer?quoteBuffer.merge([],windowStart(),incomingKey):[];
    }
    const state=result.state||{}, q=(latestLive&&result.quote&&latestLive.key===result.quote.key&&latestLive.time_ms>result.quote.time_ms?latestLive:result.quote);
    const report=result.position_report;
    const display=x=>x===null||x===undefined?'—':escape(x);
    const table=(headers,rows)=>`<table class="records-table"><thead><tr>${headers.map(x=>`<th>${x}</th>`).join('')}</tr></thead><tbody>${rows.map(row=>`<tr>${row.map(x=>`<td>${display(x)}</td>`).join('')}</tr>`).join('')}</tbody></table>`;
    el('platform-positions').innerHTML=report?
      `<p class="note">${escape(dateTime(report.time_ms))} · ${escape(report.status)}（上次对账快照）</p><h3>币安 ${escape(report.symbol)}</h3>`+
      (report.binance.length?table(['方向','数量 XAU','开仓均价 USDT','未实现盈亏 USDT'],report.binance.map(x=>[x.positionSide==='BOTH'?(Number(x.positionAmt)<0?'空头':'多头'):x.positionSide,Math.abs(Number(x.positionAmt)),x.entryPrice,x.unRealizedProfit])):'<p class="note">当前品种无持仓。</p>')+
      `<h3>MT5 ${escape(report.mt5_symbol)}</h3>`+
      (report.mt5.length?table(['票据','方向','手数','开仓价','开仓时间','盈亏 '+escape(report.mt5_currency),'归属'],report.mt5.map(x=>[x.ticket,x.side===0?'买入':'卖出',x.lots,x.price_open,x.time_ms?dateTime(x.time_ms):'—',x.profit,x.managed?'本策略':'未接管'])):'<p class="note">当前品种无持仓。</p>')+
      '<h3>币安未成交委托</h3>'+(report.binance_orders.length?table(['订单号','方向','类型','委托数量','已成交数量','委托价','状态'],report.binance_orders.map(x=>[x.orderId,x.side,x.type,x.origQty,x.executedQty,x.price,x.status])):'<p class="note">当前品种无挂单。</p>'):
      '<p class="note">尚无实盘持仓快照；连接实盘后点击“持仓对账”读取。纸面模式不读取真实账户持仓。</p>';
    const mode=result.capabilities?.mode||'paper';
    const reconciled=result.capabilities?.reconciled === true;
    const positionMode=result.capabilities?.position_mode;
    const positionLabel=positionMode==='hedge'?'双向持仓':positionMode==='one_way'?'单向持仓':'';
    const runtimeEntry=Number(result.strategy_runtime?.entry_spread_usd);
    const effectiveEntry=el('entry-effective');
    if(effectiveEntry) {
      effectiveEntry.dataset.value=Number.isFinite(runtimeEntry)?String(runtimeEntry):'';
      updateEntryLabel();
    }
    const pf=result.entry_preflight;
    if(pf) {
      const labels={ready:'预检通过',checking:'预检中',failed:'预检失败',expired:'预检已过期',waiting:'等待接近阈值'};
      text('entry-preflight',`${labels[pf.state]||pf.state}：${pf.message||'—'}${pf.age_ms!=null?' · '+pf.age_ms+' ms前':''}`,pf.state==='failed');
    } else {
      text('entry-preflight','接近阈值时自动预检；通过预检不代表保证成交。');
    }
    el('trading-mode').textContent=result.connected ? `${mode} 已连接${positionLabel?' · '+positionLabel:''}` : '未连接';
    el('trading-mode').classList.toggle('warning', Boolean(state.alarm||result.last_error||(!reconciled&&result.connected)));
    el('trading-mode').classList.toggle('neutral', !result.connected);
    const connectButton=el('trading-connect');
    connectButton.textContent=result.connected?'保存并重新连接':'保存并连接双边行情';
    connectButton.classList.toggle('primary',!result.connected);
    streamLabel();
    const oldManaging=(state.groups||[]).some(g=>g.imported&&g.status!=='closed'&&g.management_enabled);
    const groups=state.groups||[];
    const protectionPending=groups.some(g=>['unwinding','closing'].includes(g.status));
    const closeLabel=groups.some(g=>g.status==='unwinding')?'保护性平仓':'双边平仓';
    if(reconcileError&&reconciled&&!state.recovery&&!groups.some(g=>g.status!=='closed'&&valuationPending(g,state.orders||[]))) {
      actionError='';reconcileError=false;
    }
    const pause=state.last_pause;
    const currentPause=pause&&Number(pause.time_ms)>=Math.max(Number(state.boot_ms)||0,Number(state.last_start_ms)||0);
    const rawAlarm=state.alarm||result.last_error||(currentPause?pause.reason_label:'');
    // A rollback can finish after the pause alarm was written. Keep the
    // durable group reason in the history, but do not present the old
    // "正在保护性平仓" text as if an exposure were still open.
    const staleProtectionAlarm=groups.every(g=>g.status==='closed')&&/正在保护性平仓/.test(String(rawAlarm||''));
    const alarm=staleProtectionAlarm
      ? '上一笔异常已完成保护性处理，当前等待手动恢复自动开仓'
      : rawAlarm;
    const paused=!state.enabled&&(currentPause||Boolean(state.alarm));
    const manualPause=currentPause&&pause.reason_code==='USER_PAUSE';
    const pausedAt=currentPause&&Number.isFinite(Number(pause.time_ms))
      ? `（${new Date(Number(pause.time_ms)).toLocaleTimeString('zh-CN',{hour12:false})}）` : '';
    const stateText = !result.connected ? '未连接，不能开仓'
      : (!reconciled || state.recovery) ? '已连接，等待持仓对账'
      : protectionPending ? `已连接，自动开仓已暂停：${closeLabel}处理中（尚未确认双平）`
      : state.enabled ? '自动开仓运行中'+(oldManaging?' · 旧仓管理运行中':'')
      : paused ? `已连接，${manualPause?'已手动暂停新开仓':'自动开仓已暂停'}${pausedAt}`+(oldManaging?' · 旧仓管理运行中':'')
      : oldManaging ? '旧仓管理运行中 · 新开仓未启动'
      : '已连接，尚未启动自动开仓';
    const message=result.message ? `\n${result.message}` : '';
    const next=(!result.connected ? '请先连接行情。'
      : ((!reconciled||state.recovery) ? (result.external_close_status?.message||'请先点击“持仓对账”，对账通过后才能启动自动开仓。')
      : protectionPending ? '平仓完成前不能启动自动开仓；若有成交待确认，先核对再处理已有敞口。'
      : paused ? '已有仓位继续按规则管理。核对平台持仓和暂停原因后，点击“启动自动开仓”手动恢复；程序不会自动恢复开新仓。'
      : result.capabilities?.live_orders ? '实盘通道已连接、持仓已对账；启动时仍会检查配置和策略状态。'
      : '纸面模式不会发送真实订单。'));
    text('trading-result', `${actionError? actionError+'\n' : ''}${stateText}${alarm?'：'+alarm:''}${message}\n${next}${result.plan?`\n每组：MT5 ${result.plan.lots} 手 ↔ 币安 ${result.plan.qty} 盎司。`: ''}`,Boolean(alarm||actionError));
    const auto=result.auto_values||{}, details=[];
    if(auto.fx?.value) {
      if(loaded && el('auto-fx').checked && !dirtyFields.has('usdt-fx')) el('usdt-fx').value=Number(auto.fx.value).toFixed(6);
      details.push(`USDT/USD ${Number(auto.fx.value).toFixed(6)}（币安资产指数）`);
    }
    if(auto.binance_fee?.taker!==undefined) {
      if(loaded && el('auto-binance-fee').checked && el('exec-mode').value==='live' && !dirtyFields.has('binance-fee')) el('binance-fee').value=Number(auto.binance_fee.taker).toFixed(4);
      details.push(`币安 taker ${Number(auto.binance_fee.taker).toFixed(4)}%`);
    }
    if(auto.mt5_margin_check) {
      const m=auto.mt5_margin_check;
      details.push(`MT5 最大总手数 ${n(m.lots)} 手保证金 ${Number(m.required).toFixed(2)} / 可用 ${Number(m.available).toFixed(2)} USD`);
    }
    if(auto.binance_permission_warning) details.push(`币安权限明细读取待复核：${auto.binance_permission_warning}`);
    if(auto.binance_transport) details.push('币安行情 '+auto.binance_transport);
    if(auto.mt5_transport) details.push('MT5 '+auto.mt5_transport);
    for(const key of ['fx_warning','carry_warning']) if(auto[key]) details.push(auto[key]);
    if(details.length) text('cost-source',details.join(' · '),Boolean(auto.fx_warning||auto.carry_warning));
    
    el('trading-toggle').textContent='启动自动开仓';
    
    showQuote(q);
    if(result.plan) {
      const p=result.plan, notional=q ? p.qty*q.binance.bid : null;
      text('runtime-plan',`引擎当前每组：MT5 ${n(p.lots)} 手 ↔ 币安 ${n(p.qty)} XAU（${result.capabilities?.mode==='live'?'实盘':'纸面'}）`);
      if(typeof previewPlan==='function') {
        const same=Number(el('lots').value)===Number(p.lots)&&!result.config_pending&&
          (!result.strategy_runtime?.mt5_symbol||el('mt5-symbol').value===result.strategy_runtime.mt5_symbol)&&
          (!result.strategy_runtime?.symbol||el('symbol').value.toUpperCase()===result.strategy_runtime.symbol)&&
          ![...dirtyFields].some(id=>id==='lots'||sectionFields.mt5.has(id)||id==='symbol');
        if(same)showPlan({...p,mt5_lots:p.lots,binance_qty_xau:p.qty,gold_qty_oz:p.qty,binance_notional_usdt:notional},true);
        else previewPlan();
      }
    } else {
      text('runtime-plan','引擎尚未连接；上方为当前输入预览。');
    }
    updateWorkflow();
    renderPositionFreshness();
    renderIncome(q);
    el('orders').innerHTML=orderRows(state.orders||[],state.groups||[]);
    renderEvents(result.events||[]);
    renderAdoption(result);
    renderExitEditor();
  }
  const ruleInputs={target_mode:'mode',take_contraction_usd:'take',exit_spread_usd:'spread',require_net_profit:'require',min_net_profit_usd:'profit',group_loss_enabled:'loss-enabled',group_max_loss_usd:'loss',max_hold_minutes:'hold'};
  let exitDraft=null, exitPreview=null, exitChoicesSignature='';
  function exitChoices() {
    const active=(last?.state?.groups||[]).filter(g=>!g.imported&&g.status!=='closed');
    const options=active.filter(g=>g.status==='open'&&g.parameters?.exit_mode==='group').map(g=>({value:'group:'+g.id,groups:[g],label:'#'+g.id+' · 逐组退出'}));
    const basket=active.filter(g=>g.parameters?.exit_mode==='basket');
    if(basket.length&&basket.every(g=>g.status==='open'))options.push({value:'basket',groups:basket,label:'整篮子 · 当前全部 '+basket.length+' 组'});
    return options;
  }
  const savedExitRules=g=>Object.fromEntries(Object.keys(ruleInputs).map(key=>[key,g.parameters?.[key]]));
  const rebateInputs={binance_rebate_percent:'binance-rebate',mt5_rebate_percent:'mt5-rebate'};
  const savedRebates=g=>Object.fromEntries(Object.keys(rebateInputs).map(key=>[key,g.costs?.[key]||0]));
  const describeRebates=r=>`预计手续费返佣：币安 ${n(r.binance_rebate_percent)}% / MT5 ${n(r.mt5_rebate_percent)}%；计入净收益及平仓判断，非已到账`;
  function readRebates(){
    return Object.fromEntries(Object.entries(rebateInputs).map(([key,id])=>{
      const value=el('exit-rule-'+id).value;
      if(!value.trim()||!Number.isFinite(Number(value))||Number(value)<0||Number(value)>100)throw Error('返佣比例请填 0～100；30 表示手续费的 30%。');
      return [key,Number(value)];
    }));
  }
  function exitDraftCurrent() {
    if(!exitDraft||!last?.connected||!last.capabilities?.reconciled||last.state?.recovery||(last.state?.orders||[]).some(o=>o.result?.status!=='done'))return false;
    const current=exitChoices().find(c=>c.value===exitDraft.choice);
    return Boolean(current&&current.groups.length===exitDraft.groups.length&&current.groups.every(g=>
      exitDraft.groups.some(old=>old.id===g.id&&old.revision===(g.exit_rules_revision||0)&&JSON.stringify(old.rules)===JSON.stringify(savedExitRules(g))&&JSON.stringify(old.rebates)===JSON.stringify(savedRebates(g)))));
  }
  function renderExitEditor() {
    const options=exitChoices(),select=el('exit-rule-group'),signature=options.map(x=>x.value+':'+x.groups.map(g=>g.id).sort().join(',')).join(';');
    if(signature!==exitChoicesSignature) {
      const value=select.value;
      select.innerHTML='<option value="">请选择已有组</option>'+options.map(x=>`<option value="${escape(x.value)}">${escape(x.label)}</option>`).join('');
      select.value=options.some(x=>x.value===value)?value:'';exitChoicesSignature=signature;
    }
    const available=Boolean(last?.connected&&last.capabilities?.reconciled&&!last.state?.recovery&&!(last.state?.orders||[]).some(o=>o.result?.status!=='done'));
    el('exit-rule-load').disabled=busy||!available||!select.value;
    const current=exitDraftCurrent();
    el('exit-rule-preview').disabled=busy||!current;
    el('exit-rule-apply').disabled=busy||!current||!exitPreview;
    if(exitDraft&&!current&&!exitDraft.stale){exitDraft.stale=true;exitPreview=null;text('exit-rule-result','成员、状态或规则已变化，请重新载入再预览。',true);}
  }
  function readExitRules() {
    const rules={};
    for(const [key,id] of Object.entries(ruleInputs)) {
      const value=el('exit-rule-'+id).value;
      if(key==='target_mode')rules[key]=value;
      else if(['require_net_profit','group_loss_enabled'].includes(key)){
        if(!['true','false'].includes(value))throw Error('请明确选择退出条件开关');
        rules[key]=value==='true';
      }
      else {if(!value.trim()||!Number.isFinite(Number(value)))throw Error('请完整填写有限数值；无持仓时限请填 0。');rules[key]=Number(value);}
    }
    const bounds={take_contraction_usd:[.000001,100000],exit_spread_usd:[-100000,100000],min_net_profit_usd:[0,1e9],group_max_loss_usd:[.01,1e9],max_hold_minutes:[0,525600]};
    if(!['contraction','absolute'].includes(rules.target_mode))throw Error('请选择止盈方式');
    for(const [key,[lo,hi]] of Object.entries(bounds))if(rules[key]<lo||rules[key]>hi)throw Error('退出条件数值超出允许范围，请核对各输入。');
    return rules;
  }
  function describeExitRules(r) {
    return `${r.target_mode==='absolute'?'退出价差 ≤ '+n(r.exit_spread_usd):'价差收窄 ≥ '+n(r.take_contraction_usd)} USD/盎司；${r.require_net_profit?'最低净收益 '+usd(r.min_net_profit_usd):'不要求最低净收益'}；${r.group_loss_enabled?'单组最大亏损 '+usd(r.group_max_loss_usd):'单组亏损限制关闭'}；${r.max_hold_minutes?'从原开仓起 '+n(r.max_hold_minutes)+' 分钟':'不限持仓时间'}`;
  }
  el('exit-rule-group').onchange=()=>{exitDraft=null;exitPreview=null;el('exit-rule-fields').hidden=true;text('exit-rule-result','选择后请载入当前规则。');renderExitEditor();};
  el('exit-rule-load').onclick=()=>{
    const choice=exitChoices().find(x=>x.value===el('exit-rule-group').value);if(!choice)return;
    exitDraft={choice:choice.value,groups:choice.groups.map(g=>({id:g.id,revision:g.exit_rules_revision||0,rules:savedExitRules(g),rebates:savedRebates(g)}))};exitPreview=null;
    const rules=exitDraft.groups[0].rules;
    for(const [key,id] of Object.entries(ruleInputs))el('exit-rule-'+id).value=String(rules[key]??'');
    for(const [key,id] of Object.entries(rebateInputs))el('exit-rule-'+id).value=String(exitDraft.groups[0].rebates[key]);
    el('exit-rule-fields').hidden=false;
    text('exit-rule-result','已载入 '+choice.label+'。篮子按第一组回填，预览会逐组列出原规则；确认前不会修改。');renderExitEditor();
  };
  const invalidateExitPreview=()=>{exitPreview=null;el('exit-rule-apply').disabled=true;text('exit-rule-result','条件已编辑，请重新预览修改。');};
  el('exit-rule-fields').addEventListener?.('input',invalidateExitPreview);
  el('exit-rule-fields').addEventListener?.('change',invalidateExitPreview);
  el('exit-rule-preview').onclick=()=>{
    try {
      if(!exitDraftCurrent())throw Error('成员、状态或规则已变化，请重新载入');
      const rules=readExitRules(),rebates=readRebates();
      exitPreview={groups:exitDraft.groups.map(g=>g.id),expected_revisions:Object.fromEntries(exitDraft.groups.map(g=>[g.id,g.revision])),rules,rebates};
      text('exit-rule-result',exitDraft.groups.map(g=>'#'+g.id+' 原规则：'+describeExitRules(g.rules)+'；'+describeRebates(g.rebates)).join('\n')+'\n统一修改为：'+describeExitRules(rules)+'；'+describeRebates(rebates)+'\n仅影响以上已有组；总亏损限制及滑点保护保持原值。持仓时限从原开仓时间计，不重新计时。确认后下一次有效行情检查起生效，条件已满足时可能立即触发平仓。');
      renderExitEditor();
    } catch(error){exitPreview=null;text('exit-rule-result',error.message,true);renderExitEditor();}
  };
  el('exit-rule-apply').onclick=()=>action(async()=>{
    if(!exitPreview||!exitDraftCurrent()||JSON.stringify(readExitRules())!==JSON.stringify(exitPreview.rules)||JSON.stringify(readRebates())!==JSON.stringify(exitPreview.rebates))throw Error('预览已失效，请重新载入或预览');
    const payload=exitPreview;
    const result=await request('/api/trading/exit-rules',payload);
    exitDraft=null;exitPreview=null;render(result);el('exit-rule-fields').hidden=true;text('exit-rule-result',result.message);
    paintSoon();
  },'exit-rule-apply','正在保存所选已有组的退出规则…');
  function renderIncome(q) {
    if(!last)return;
    const orders=last.state?.orders||[],raw=last.state?.groups||[];
    const guarded=raw.map(g=>valuationPending(g,orders)?{...g,valuation_status:'unverified'}:g);
    const groups=window.GoldPairQuotes?.markGroups?window.GoldPairQuotes.markGroups(guarded,last.quote,q):guarded;
    const active=groups.filter(g=>g.status!=='closed'),closed=groups.filter(g=>g.status==='closed');
    const sum=(items,key)=>items.reduce((total,g)=>total+Number(g.valuation?.[key]||0),0);
    const allCarry=sum(groups,'carry'),allFees=sum(groups,'fees')+sum(active,'estimated_exit_fee');
    const pending=active.some(g=>valuationPending(g,orders));
    const activeRebate=sum(active,'estimated_rebate'),closedRebate=sum(closed,'estimated_rebate');
    const stats=[[pending?'收益待核对':activeRebate>0?'实时净收益（含预计返佣）':'实时净收益',pending?'—':usd(sum(active,'net'))],['已平仓净收益'+(closedRebate>0?'（含预计返佣）':''),usd(sum(closed,'net'))],['累计资金费 + Swap（含已平仓）',usd(allCarry)],['手续费及预估平仓费',pending?'待核对':usd(-allFees)]];
    if(activeRebate+closedRebate>0||groups.some(g=>Number(g.costs?.binance_rebate_percent)>0||Number(g.costs?.mt5_rebate_percent)>0))stats.push(['预计返佣（未确认到账）',pending?'待核对':usd(activeRebate+closedRebate)]);
    el('pnl-stats').innerHTML=stats.map(([label,value])=>`<div class="stat"><span>${label}</span><strong>${value}</strong></div>`).join('');
    const container=el('groups');
    for(const detail of container.querySelectorAll('details[data-group-rules]')) {
      if(detail.open)expandedRules.add(detail.dataset.groupRules);else expandedRules.delete(detail.dataset.groupRules);
    }
    for(const id of expandedRules)if(!groups.some(g=>g.id===id))expandedRules.delete(id);
    const focused=document.activeElement?.closest?.('details[data-group-rules]')?.dataset.groupRules;
    container.innerHTML=groupRows(groups,orders);
    if(focused)for(const detail of container.querySelectorAll('details[data-group-rules]')) {
      if(detail.dataset.groupRules===focused)detail.querySelector('summary')?.focus({preventScroll:true});
    }
    for(const button of el('groups').querySelectorAll('[data-close]')) button.onclick=()=>action(()=>closeOne(button.dataset.close),'group-close','平仓请求处理中；尚未确认双平…');
    for(const button of el('groups').querySelectorAll('[data-edit-exit]')) button.onclick=()=>{
      const group=(last.state?.groups||[]).find(g=>g.id===button.dataset.editExit);if(!group)return;
      const panel=el('exit-rules-panel');panel.open=true;
      el('exit-rule-group').value=group.parameters.exit_mode==='basket'?'basket':'group:'+group.id;
      el('exit-rule-group').onchange();
      if(!el('exit-rule-load').disabled)el('exit-rule-load').onclick();
      else text('exit-rule-result','当前状态不允许修改，请先处理待确认成交并完成持仓对账。',true);
      panel.scrollIntoView?.({block:'start'});
    };
    const imported=groups.find(g=>g.imported&&g.status!=='closed');
    if(imported)showAdoptionSummary(imported);
  }
  function showAdoptionSummary(group) {
    const managing=Boolean(group.management_enabled);
      el('adoption-managed-status').textContent=`接管篮子 #${group.id} · ${managing?'管理已启动':'管理已暂停'} · ${group.status==='open'?'持仓中':group.status} · 数量 ${n(group.qty)} XAU / ${n(group.lots)} 手 · 开仓参考价差 ${n(group.entry)} USD/盎司 · 净收益 ${usd(group.valuation?.net)}（估算）`;
  }
  function renderAdoption(result) {
    const group=(result.state?.groups||[]).find(g=>g.imported&&g.status!=='closed');
    el('adoption-setup').hidden=Boolean(group);el('adoption-managed').hidden=!group;
    if(group) {
      if(adoptionGroupId!==group.id) {
        el('adoption-managed-take').value=group.parameters.take_contraction_usd;
        el('adoption-managed-profit').value=group.parameters.min_net_profit_usd;
        adoptionGroupId=group.id;
      }
      const managing=Boolean(group.management_enabled);
      el('adoption-manage').textContent=managing?'暂停已有仓位管理':'启动已有仓位管理';
      el('adoption-managed-take').disabled=managing;el('adoption-managed-profit').disabled=managing;
      showAdoptionSummary(group);
    } else adoptionGroupId=null;
    const report=result.position_report;
    const binanceSummary=el('adoption-binance-summary');
    if(binanceSummary) {
      if(!report) binanceSummary.textContent='读取后显示币安配平仓位。';
      else if(!report.binance?.length) binanceSummary.textContent='币安当前品种没有持仓，暂时不能接管。';
      else {
        const mode=result.capabilities?.position_mode==='hedge'?'双向持仓':'单向持仓';
        const rows=report.binance.map(x=>{
          const amount=Math.abs(Number(x.positionAmt)||0);
          const side=x.positionSide==='BOTH'?(Number(x.positionAmt)<0?'空头':'多头'):x.positionSide;
          return `${side} ${n(amount)} XAU，均价 ${n(x.entryPrice)} USDT`;
        });
        binanceSummary.textContent=`币安当前仓位（${mode}）：${rows.join('；')}。接管要求只有对应空头，且数量必须与所选 MT5 票据配平。`;
      }
    }
    if(report && report.time_ms!==adoptionReportTime) {
      const selected=new Set(Array.from(el('adoption-tickets').querySelectorAll('input:checked'),x=>x.value));
      el('adoption-tickets').innerHTML=report.mt5.filter(p=>p.side===0&&!p.managed).map(p=>`<label class="check"><input type="checkbox" value="${escape(p.ticket)}" ${selected.has(String(p.ticket))?'checked':''} />票据 ${escape(p.ticket)} · ${n(p.lots)} 手 · 开仓 ${n(p.price_open)} · ${dateTime(p.time_ms)}</label>`).join('')||'没有可接管的 MT5 多头票据。';
      adoptionReportTime=report.time_ms;
      invalidateAdoption();
    }
  }
  function invalidateAdoption() {
    adoptionRevision++;
    adoptionPreview=null;el('adoption-confirm').disabled=true;
    el('adoption-preview-result').textContent='选择或条件变化后，请重新预览。';
  }
  el('adoption-setup').addEventListener?.('input',invalidateAdoption);
  el('adoption-setup').addEventListener?.('change',invalidateAdoption);
  el('adoption-read').onclick=()=>action(async()=>{
    try {render(await request('/api/trading/reconcile',{}));}
    catch(error) {await status(false);throw error;}
  },'adoption-read','正在读取平台持仓…');
  el('adoption-preview').onclick=()=>action(async()=>{
    invalidateAdoption();
    const revision=adoptionRevision;
    const result=await request('/api/trading/adoption/preview',{
      tickets:Array.from(el('adoption-tickets').querySelectorAll('input:checked'),x=>x.value),
      entry_fx:el('adoption-fx').value,history_fees_usd:el('adoption-fees').value,
      history_funding_usd:el('adoption-funding').value,costs_confirmed:el('adoption-costs-confirmed').checked,
      take_contraction_usd:el('adoption-take').value,min_net_profit_usd:el('adoption-profit').value
    });
    if(revision!==adoptionRevision)throw Error('预览期间输入已变化，请重新预览');
    const p=result.preview;adoptionPreview=p;
    el('adoption-preview-result').textContent=`MT5 ${p.positions.map(x=>x.ticket).join('、')} 共 ${n(p.lots)} 手 ↔ 币安全部空头 ${n(p.qty)} XAU。\n币安原均价 ${n(p.binance_entry)} USDT × 汇率 ${p.entry_fx} − MT5 加权开仓均价 ${n(p.mt5_entry)} USD = 开仓参考价差 ${n(p.entry)} USD/盎司。\n历史手续费 ${p.history_fees_usd} USD；历史资金费净收入 ${p.history_funding_usd} USD；MT5 Swap 按平台读取。\n退出：收窄至少 ${p.parameters.take_contraction_usd} USD/盎司，且篮子净收益 ≥ ${p.parameters.min_net_profit_usd} USD（估算）。\n独立旧仓篮子，不补仓。预览 2 分钟有效，确认时再次核验持仓；确认本身不下单。`;
  },'adoption-preview','正在核验配平数量…');
  el('adoption-confirm').onclick=()=>action(async()=>{
    if(!adoptionPreview)throw Error('请先生成预览');
    try {render(await request('/api/trading/adoption/confirm',{preview_id:adoptionPreview.id}));invalidateAdoption();}
    catch(error) {await status(false);throw error;}
  },'adoption-confirm','正在登记接管（不下单）…');
  el('adoption-manage').onclick=()=>action(async()=>{
    const group=(last?.state?.groups||[]).find(g=>g.id===adoptionGroupId);
    if(!group)throw Error('未找到接管篮子');
    render(await request('/api/trading/adoption/manage',{group:group.id,enabled:!group.management_enabled,
      take_contraction_usd:el('adoption-managed-take').value,min_net_profit_usd:el('adoption-managed-profit').value}));
  },'adoption-manage','正在切换旧仓管理…');
  async function plot() {
    if(document.hidden||el('view-trading')?.hidden)return;
    const generation=++plotGeneration;
    const revision=historyRevision;
    const requestedKey=last?.quote?.key;
    plotController?.abort();
    const controller=new AbortController();plotController=controller;
    try {
      const minutes=Number(el('chart-window').value);
      const r=await request('/api/trading/chart?minutes='+minutes,undefined,controller);
      if(document.hidden||generation!==plotGeneration||requestedKey!==last?.quote?.key||minutes!==Number(el('chart-window').value))return;
      // Keep ticks received during the history request; merge only once per paint.
      chartSamples=quoteBuffer?quoteBuffer.replaceHistory(r.samples||[],windowStart(),last?.quote?.key):(r.samples||[]);
      historyDirty=revision!==historyRevision;historyLoadedAt=Date.now();
      drawChart(chartSamples);
    } catch(error) {
      if(generation===plotGeneration&&!document.hidden)throw error;
    } finally {if(plotController===controller)plotController=null;}
  }
  function drawChart(rawSamples) {
    const windowMinutes=Math.max(1,Number(el('chart-window').value)||1440);
    const rangeEnd=Date.now(),rangeStart=rangeEnd-windowMinutes*60000;
    const series=window.GoldPairQuotes?.chartSeries?window.GoldPairQuotes.chartSeries(rawSamples,windowMinutes*60000,chartView):{samples:rawSamples,lines:rawSamples,gaps:[]};
    const {samples,lines,gaps}=series;
    if(!chart) {
      chart=echarts.init(el('spread-chart'));
      if(chart.on)chart.on('datazoom',()=>{
        const zoom=chart.getOption()?.dataZoom?.[0];
        const axis=chart.getOption()?.xAxis?.[0];
        const first=axis?.min??rangeStart,lastTime=axis?.max??rangeEnd;
        chartView={start:first+(lastTime-first)*(zoom?.start||0)/100,end:first+(lastTime-first)*(zoom?.end??100)/100};
        paintSoon();
      });
    }
    const groups=last?.state?.groups||[];
    const marks=groups.filter(g=>g.status==='open').map(g=>({name:(g.imported?'旧仓 ':'')+'#'+g.id+(g.imported?(g.management_enabled?' 接管止盈':' 目标（暂停）'):' 已锁定止盈'),yAxis:g.parameters?.target_mode==='absolute'?g.parameters.exit_spread_usd:g.entry-g.parameters?.take_contraction_usd})).filter(mark=>Number.isFinite(mark.yAxis));
    const missing=gaps.map(gap=>[{name:gap.reason,xAxis:gap.start,itemStyle:{color:'rgba(100,116,139,.16)'},label:{show:true,color:'#536274',position:'insideTop'}},{xAxis:gap.end}]);
    text('chart-gaps',gaps.length?`发现 ${gaps.length} 处无有效采样区间，已用灰色标出；最近一处 ${date(gaps.at(-1).start)} 至 ${date(gaps.at(-1).end)}。可点“回到最新”补读本机历史；停机或断线期间未采集的报价无法补造。`:'当前已加载的采样未发现明显缺口；停留后台后会自动补读本机记录。');
    const nearest=(at,field)=>{let best=null,distance=Infinity;for(const x of samples){const d=Math.abs(x.time_ms-at);if(d<distance){best=x;distance=d;}}return best&&Number.isFinite(best[field])?[at,best[field]]:null;};
    const firstTime=samples[0]?.time_ms??0,lastTime=samples.at(-1)?.time_ms??0;
    const opened=groups.filter(g=>!g.imported&&g.opened_ms>=firstTime&&g.opened_ms<=lastTime).map(g=>({name:'#'+g.id,value:[g.opened_ms,g.entry]}));
    const closed=groups.filter(g=>g.closed_ms>=firstTime&&g.closed_ms<=lastTime).map(g=>({name:'#'+g.id,value:[g.closed_ms,Number.isFinite(g.exit)?g.exit:nearest(g.closed_ms,'exit')?.[1]]})).filter(x=>Number.isFinite(x.value[1]));
    const entries=samples.map(x=>x.entry).filter(Number.isFinite), exits=samples.map(x=>x.exit).filter(Number.isFinite);
    const average=values=>values.length?values.reduce((a,b)=>a+b,0)/values.length:null;
    const latest=rawSamples.at(-1), all=entries.concat(exits);
    const effective=last?.connected?last.strategy_runtime?.entry_spread_usd:null;
    const threshold=Number.isFinite(effective)?effective:null;
    el('spread-stats').innerHTML=[['当前入场',latest?.entry],['当前退出',latest?.exit],['开仓阈值',threshold],['窗口入场均值',average(entries)],['价差范围',all.length?`${Math.min(...all).toFixed(3)} ～ ${Math.max(...all).toFixed(3)}`:null]].map(([label,value])=>`<div class="stat"><span>${label}</span><strong>${typeof value==='number'?value.toFixed(3):value||'—'}</strong></div>`).join('');
    el('chart-empty').classList.toggle('is-hidden',samples.length>0);
    el('chart-coverage').textContent=samples.length?`本机可成交价差 ${samples.length.toLocaleString('zh-CN')} 个采样 · ${new Date(samples[0].time_ms).toLocaleString()} 至 ${new Date(samples.at(-1).time_ms).toLocaleString()} · 高频记录保留 24 小时，更早按分钟归档保留 30 天；历史保存在本机数据目录。`:'尚无本机历史；连接后开始记录真实 Bid/Ask 可成交价差，历史会保存到本机数据目录。';
    const windowLabel=windowMinutes>=1440?`${windowMinutes/1440} 天`:`${windowMinutes/60} 小时`;
    el('chart-coverage').textContent=`观察窗口：最近 ${windowLabel} · `+el('chart-coverage').textContent+
      (samples.length&&samples[0].time_ms-rangeStart>Math.max(60000,windowMinutes*60000/100)?' 所选窗口前段暂无本机记录，已留空显示；切换窗口不会补造停机前或断线期间的行情。':'');
    const oldZoom=chart.getOption()?.dataZoom?.[0];
    const start=resetChartZoom?0:(oldZoom?.start??0),end=resetChartZoom?100:(oldZoom?.end??100);resetChartZoom=false;
    const thresholdLine=threshold===null?[]:[{name:`开仓阈值 ${threshold.toFixed(2)}`,yAxis:threshold,lineStyle:{color:'#b75e24',width:2,type:'dashed'},label:{show:true,color:'#8a461e',formatter:`开仓阈值 ${threshold.toFixed(2)}`}}];
    chart.setOption({animation:false,legend:{top:5,type:'scroll'},tooltip:{trigger:'axis',confine:true,valueFormatter:v=>Number.isFinite(v)?v.toFixed(3):'—'},grid:{left:75,right:120,top:70,bottom:80},xAxis:{type:'time',min:rangeStart,max:rangeEnd,minInterval:1000,axisPointer:{label:{formatter:params=>dateTime(params.value)}}},yAxis:{type:'value',scale:true,name:'价差 USD/盎司'},dataZoom:[{type:'inside',filterMode:'none',minValueSpan:1000,start,end},{type:'slider',filterMode:'none',minValueSpan:1000,bottom:15,start,end}],series:[
      {id:'entry',name:'入场（卖币安Bid / 买MT5Ask）',type:'line',showSymbol:false,connectNulls:false,data:lines.map(x=>[x.time_ms,x.entry]),itemStyle:{color:'#246b92'},lineStyle:{width:2,type:'solid'},markArea:{silent:true,itemStyle:{color:'rgba(36,107,146,.07)'},data:[...(threshold===null?[]:[[{name:'开仓触发区',yAxis:threshold},{yAxis:'max'}]]),...missing]},markLine:{symbol:'none',silent:true,data:[...thresholdLine,...marks],label:{formatter:'{b}: {c}'}}},
      {id:'exit',name:'退出（买回币安Ask / 卖MT5Bid）',type:'line',showSymbol:false,connectNulls:false,data:lines.map(x=>[x.time_ms,x.exit]),itemStyle:{color:'#247b68'},lineStyle:{width:2,type:'dashed'}},
      {id:'open',name:'策略开仓',type:'scatter',symbol:'triangle',symbolSize:12,itemStyle:{color:'#b75e24'},data:opened},
      {id:'close',name:'策略平仓',type:'scatter',symbol:'diamond',symbolSize:12,itemStyle:{color:'#6b50a6'},data:closed}
    ]},{notMerge:true,lazyUpdate:true});
  }
  async function status(withChart=true) {
    if(document.hidden||statusBusy)return;
    statusBusy=true;
    try {
      const r=await request('/api/trading/status');
      if(document.hidden||(window.goldPairUiBusy&&!busy)) return;
      render(r);
      // Keep the last chart visible when disconnected, but do not keep
      // re-fetching it as if live monitoring were still running.
      // Refill archived display samples as the bounded live cache rolls over.
      // This is a read-only local snapshot, independent of platform requests.
      if(withChart && !plotController && (historyDirty || !chart ||
          (r.connected && (!streamReady || Date.now()-historyLoadedAt>=60000)))) await plot();
    } finally { statusBusy=false; }
  }
  async function saveBeforeTrading() {
    const before=typeof revision==='number'?revision:null;
    await saveConfig();
    if(before!==null&&before!==revision)throw Error('保存期间参数又有修改，请核对当前输入后再次操作。');
  }
  async function connect() {
    await saveBeforeTrading();
    const r=await request('/api/trading/connect',{api_key:el('trading-api-key').value.trim(),api_secret:el('trading-api-secret').value.trim(),mt5_mcp_token:el('mt5-mcp-token').value.trim()});
    render(r);await plot();
  }
  async function closeOne(group) { render(await request('/api/trading/close',{group,reason:'用户请求平仓'})); }
  el('trading-connect').onclick=()=>action(connect,'trading-connect','正在检查行情、账户和合约规则…');
  el('trading-reconcile').onclick=()=>action(async()=>{try {render(await request('/api/trading/reconcile',{}));await plot();} catch(error) {await status(false).catch(()=>{});throw error;}},'trading-reconcile','正在核对两边持仓…');
  el('trading-toggle').onclick=()=>action(async()=>{
    if(last?.state?.enabled||pauseBusy)return;
    await saveBeforeTrading();
    render(await request('/api/trading/start',{}));await plot();
  },'trading-toggle','正在启动自动开仓…');
  // Pausing remains available during other UI operations. The backend still
  // serializes state changes with an order already being processed.
  el('trading-pause').onclick=async()=>{
    if(pauseBusy||!last?.connected||!last?.state?.enabled)return;
    pauseBusy=true;actionError='';updateWorkflow();
    try {render(await request('/api/trading/pause',{}));text('trading-result','新开仓已暂停；已有仓位继续管理，已发订单继续核对。');}
    catch(error){actionError='暂停尚未确认：'+error.message;await status(false).catch(()=>{});text('trading-result',actionError,true);}
    finally {pauseBusy=false;updateWorkflow();}
  };
  el('entry').addEventListener('input',updateEntryLabel);
  el('apply-entry').onclick=()=>action(async()=>{
    if(typeof saveQueue!=='undefined')await saveQueue.catch(()=>{});
    render(await request('/api/trading/apply-entry',{entry_spread_usd:el('entry').value}));
    paintSoon();
  },'apply-entry','正在应用开仓阈值…');
  el('trading-close').onclick=()=>action(async()=>{render(await request('/api/trading/close',{reason:'用户请求全部平仓'}));},'trading-close','正在处理平仓请求…');
  async function reloadChart(){
    resetChartZoom=true;chartView=null;invalidateHistory();
    try {await plot();}
    catch(error){text('chart-gaps','图表历史加载失败：'+error.message,true);}
  }
  // Chart reads do not take the trading action lock; a new selection replaces
  // an older in-flight chart request even while a connection check is running.
  el('chart-window').onchange=reloadChart;
  el('chart-latest').onclick=reloadChart;
  el('push-setup').onclick=()=>action(async()=>{
    const r=await request('/api/mt5/push/setup',{});
    const settings=`LocalPort=${r.port}\r\nLocalToken=${r.token}\r\n`;
    const bytes=new Uint8Array(2+settings.length*2);bytes[0]=255;bytes[1]=254;
    for(let i=0;i<settings.length;i++){const c=settings.charCodeAt(i);bytes[2+i*2]=c&255;bytes[3+i*2]=c>>8;}
    const url=URL.createObjectURL(new Blob([bytes],{type:'application/octet-stream'}));
    const link=document.createElement('a');link.href=url;link.download='GoldPairQuotes.set';link.click();
    setTimeout(()=>URL.revokeObjectURL(url),10000);
    const asset=r.binary||r.source;
    el('push-result').innerHTML=`本机接收器已启动（127.0.0.1:${Number(r.port)}），参数文件已下载。<a href="${escape(asset)}" download>下载${r.binary?'已编译 EA':'EA 源码'}</a>。将 EA 放入 MT5 数据目录的 MQL5/Experts，挂到当前黄金图表，在“输入”中载入刚下载的 .set 文件。`;
  },'push-setup','正在准备 EA 接收器和本机参数…');
  window.addEventListener('offline',()=>{stream?.close();stream=null;streamReady=false;streamLabel();});
  window.addEventListener('online',startStream);
  function suspendPage(){
    stream?.close();stream=null;streamReady=false;
    pendingQuotes=[];
    clearTimeout(paintTimer);window.cancelAnimationFrame?.(paintFrame);
    paintPending=false;paintTimer=null;paintFrame=null;
    ++plotGeneration;plotController?.abort();plotController=null;
  }
  document.addEventListener?.('visibilitychange',()=>{
    if(document.hidden){suspendPage();return;}
    // A new stream sends the latest quote and requests one history refresh.
    // No trading start/pause/close request is sent by visibility changes.
    chart?.resize?.();startStream();
    status(false).catch(error=>text('trading-result',error.message,true));
  });
  window.addEventListener('pagehide',suspendPage);
  window.addEventListener('pageshow',()=>{if(!document.hidden)startStream();});
  window.addEventListener('beforeunload',suspendPage);
  window.addEventListener('resize',()=>chart?.resize());
  window.addEventListener('gold-workspace-changed',()=>{
    chart?.resize();paintSoon();
    if((historyDirty||!chart)&&!el('view-trading')?.hidden)plot().catch(error=>text('trading-result',error.message,true));
  });
  window.addEventListener('gold-form-changed',updateWorkflow);
  window.addEventListener('gold-config-saved',()=>status(false).catch(()=>{}));
  window.addEventListener('gold-config-ready',updateWorkflow);
  window.addEventListener('gold-busy-changed',updateWorkflow);
  setTimeout(()=>status().then(startStream).catch(error=>text('trading-result',error.message,true)),400);
  timer=setInterval(()=>{if(!document.hidden&&!busy&&!window.goldPairUiBusy)status().catch(error=>text('trading-result',error.message,true));},3000);
  setInterval(()=>{if(!document.hidden){renderRisk();renderPositionFreshness();}},1000);
})();
