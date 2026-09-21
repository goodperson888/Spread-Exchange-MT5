'use strict';
(() => {
  const el = id => document.getElementById(id);
  const text = (id, value, error=false) => { el(id).textContent=value; el(id).classList.toggle('error',error); };
  const n = x => Number(x).toLocaleString('zh-CN',{maximumFractionDigits:3});
  const date = t => t ? new Date(t).toLocaleTimeString() : '—';
  const dateTime = t => t ? new Date(t).toLocaleString('zh-CN') : '—';
  const usd = x => Number.isFinite(Number(x)) ? `${Number(x)>=0?'+':''}${Number(x).toFixed(2)} USD` : '—';
  const escape = value => String(value ?? '').replace(/[&<>'"]/g, x => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[x]));
  let chart, timer, busy=false, last=null, resetChartZoom=true, actionError='';
  let chartView=null, stream=null, streamReady=false, paintPending=false, chartSamples=[], plotGeneration=0, latestLive=null;
  const quoteBuffer=window.GoldPairQuotes?new window.GoldPairQuotes.QuoteBuffer():null;
  function windowStart(){return Date.now()-Math.max(1,Number(el('chart-window').value)||1440)*60000;}
  function paintSoon(){
    if(paintPending)return;paintPending=true;
    (window.requestAnimationFrame||((fn)=>setTimeout(fn,16)))(()=>{paintPending=false;drawChart(chartSamples);renderIncome(latestLive||last?.quote);});
  }
  function streamLabel(){
    const badge=el('chart-live-status');if(!badge)return;
    const q=latestLive||last?.quote;
    const stale=q&&(q.valid===false||Date.now()-Math.min(q.mt5?.observed_ms||q.mt5?.time_ms||0,q.binance?.observed_ms||q.binance?.time_ms||0)>10000);
    badge.textContent=!last?.connected?'未连接 · 图表保留历史':stale?'行情过期或双边不同步':streamReady?'图表长连接 · 收到报价即更新':'图表重连中 · 历史保留';
    badge.classList.toggle('warning',!streamReady||stale||!last?.connected);
  }
  function startStream(){
    if(!window.EventSource||stream)return;
    stream=new window.EventSource('/api/trading/stream');
    stream.onopen=()=>{streamReady=true;streamLabel();};
    stream.onerror=()=>{streamReady=false;streamLabel();};
    stream.addEventListener('quotes',event=>{
      try {
        const payload=JSON.parse(event.data),key=last?.quote?.key;
        const rows=(payload.samples||[]).filter(q=>!key||q.key===key);
        if(rows.length){
          latestLive=rows.at(-1);
          chartSamples=quoteBuffer.merge(rows,windowStart(),key);
          const q=latestLive,age=x=>Math.max(0,Date.now()-(x?.observed_ms||x?.time_ms||0)),sourceTime=x=>x?.source_time_ms||x?.time_ms||0;
          el('quote-latency').textContent=`MT5 ${q.mt5_transport||'行情'} · 币安 ${q.binance_transport||'行情'} · 报价年龄：MT5 ${age(q.mt5)} ms / 币安 ${age(q.binance)} ms · 本机收到时间差 ${Math.abs((q.mt5?.observed_ms||0)-(q.binance?.observed_ms||0))} ms · 原始时间差 ${Math.abs(sourceTime(q.mt5)-sourceTime(q.binance))} ms${q.valid===false?' · 当前报价不满足交易校验':''}`;
          showQuote(q);paintSoon();
        }
        streamLabel();
        if(payload.reset)plot().catch(error=>text('trading-result','恢复历史失败：'+error.message,true));
      } catch(error){text('trading-result','行情推送解析失败：'+error.message,true);}
    });
  }
  let adoptionPreview=null, adoptionReportTime=null, adoptionGroupId=null, adoptionRevision=0;

  async function request(path, body) {
    const controller=new AbortController();
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
    for(const id of ['trading-connect','trading-reconcile','trading-toggle','trading-close','adoption-read','adoption-preview','adoption-confirm','adoption-manage']) {
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
    if(busy)return; actionError=''; lock(true,activeId,label);
    text('trading-result', label||'正在处理，请稍候…');
    const adoptionAction=activeId?.startsWith('adoption-');
    if(adoptionAction)text('adoption-feedback',label||'正在处理…');
    try {
      await work();
      if(adoptionAction)text('adoption-feedback',activeId==='adoption-confirm'?'已登记接管；旧仓管理尚未启动，本次没有下单。':'操作完成，请查看下方预览或管理状态。');
    } catch(error) {
      actionError='本次操作未完成：'+error.message; text('trading-result',actionError,true);
      if(adoptionAction)text('adoption-feedback',actionError,true);
    }
    finally { lock(false,activeId); }
  }
  function groupRows(groups, orders) {
    if(!groups?.length) return '<p class="note">暂无本策略持仓或交易记录。</p>';
    const states={opening:'开仓中',open:'持仓中',closing:'平仓中',unwinding:'异常撤回',attention:'需人工处理',closed:'已平仓'};
    const rows=groups.slice().reverse().map(g => {
      const v=g.valuation||{}, remaining=v.remaining||{};
      const own=orders.filter(o=>o.group===g.id&&o.action==='open'&&Number(o.result?.qty)>0);
      const amount=leg=>own.filter(o=>o.leg===leg).reduce((sum,o)=>sum+Number(o.result.qty)*Number(o.result.price),0);
      const basis=g.imported?'接管旧仓 · 成本估算':g.mode==='paper'?'纸面估算':v.costs_verified?'平台已复核':'实盘估算';
      const funding=Number(v.binance_funding||0),swap=Number(v.mt5_swap||0);
      const carryDetail=('binance_funding' in v||'mt5_swap' in v)?`资金费 ${usd(funding)}<br>MT5 Swap ${usd(swap)}`:`合计 ${usd(v.carry||0)}`;
      const action=g.status==='closed'?'':`<button data-close="${escape(g.id)}">平仓</button>`;
      const entryLabel=g.open_binance && Number.isFinite(Number(g.entry))?`<br><span class="muted" title="币安开仓成交价 × 该笔记录的 USDT/USD − MT5 开仓成交价；双边成交可能不在同一时刻">开仓成交价差 ${n(g.entry)} USD/盎司</span>`:'';
      const gridLabel=Number(g.grid_index||0)>0?`<br><span class="muted">网格补仓第 ${Number(g.grid_index)} 次</span>`:'';
      return `<tr><td><strong>#${escape(g.id)}</strong>${gridLabel}<br><span class="muted">${dateTime(g.opened_ms)}${g.closed_ms?'<br>→ '+dateTime(g.closed_ms):''}</span></td><td>${escape(states[g.status]||g.status)}<br><span class="record-basis">${basis}</span>${g.execution_warning?'<br><span class="muted">'+escape(g.execution_warning)+'</span>':''}</td><td>${n(g.lots)} 手 / ${n(g.qty)} 盎司${entryLabel}<br><span class="muted">币安 ${amount('binance').toFixed(2)} USDT<br>MT5 ${amount('mt5').toFixed(2)} USD</span></td><td>毛收益 ${usd(v.gross)}<br>手续费 -${Number(v.fees||0).toFixed(2)} USD${Number(v.estimated_exit_fee)>0?'<br>预估平仓费 -'+Number(v.estimated_exit_fee).toFixed(2)+' USD':''}</td><td>${carryDetail}</td><td class="${Number(v.net)>=0?'pnl-positive':'pnl-negative'}"><strong>${usd(v.net)}</strong><br><span class="muted">${g.status==='closed'?'平仓净收益':'实时净收益'}</span></td><td>币安 ${n(remaining.binance||0)}<br>MT5 ${n(remaining.mt5||0)}${g.reason?'<br>'+escape(g.reason):''}</td><td>${action}</td></tr>`;
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
      const status=o.imported?'原持仓成本登记（本次未下单）':r.status==='done'?(filled>0?'已成交':'未成交'):(r.status==='pending'?'待确认':'状态未知');
      const fee=o.imported?'历史费用见接管汇总':Number.isFinite(Number(r.fee))?`${Number(r.fee).toFixed(4)} ${o.leg==='binance'?'USDT':'USD'}`:'未单独回填';
      const timing=Number.isFinite(r.fill_time_ms)?`<br><span class="muted">发送→回报 ${Math.max(0,r.fill_time_ms-o.created_ms)} ms${Number.isFinite(o.signal_time_ms)?'<br>采样→发送 '+Math.max(0,o.created_ms-o.signal_time_ms)+' ms':''}</span>`:'';
      const signal=Number.isFinite(Number(o.signal_spread))?n(o.signal_spread):'—';
      const actual=Number.isFinite(Number(o.actual_spread))?n(o.actual_spread):'等待双边成交';
      const spreadDelta=Number.isFinite(Number(o.spread_slippage))?`${Number(o.spread_slippage)>=0?'+':''}${n(o.spread_slippage)}`:'—';
      return `<tr><td>${dateTime(o.created_ms)}${timing}</td><td>#${escape(o.group)}</td><td>${o.leg==='binance'?'币安':'MT5'}<br>${escape(o.symbol)}</td><td>${o.action==='open'?'开仓':'平仓'} · ${direction}</td><td>申请 ${n(o.requested)} 盎司<br>成交 ${quantity}</td><td>${price?`${n(price)}<br>${(filled*price).toFixed(2)} ${o.leg==='binance'?'USDT':'USD'}`:'—'}</td><td>${signal}<br><span class="muted">${actual}</span><br><span class="muted">偏移 ${spreadDelta}</span></td><td>${fee}</td><td>${status}${r.error?'<br>'+escape(r.error):''}<br><span class="muted">${escape(r.ticket||o.id)}</span></td></tr>`;
    }).join('');
    return `<table class="records-table"><thead><tr><th>时间</th><th>交易组</th><th>平台 / 品种</th><th>动作</th><th>申请 / 成交数量</th><th>成交价 / 金额</th><th>监控价差<br>实际成交价差<br>偏移</th><th>成交手续费</th><th>状态 / 票据</th></tr></thead><tbody>${rows}</tbody></table>`;
  }
  function showQuote(q) {
    if(q) {
      el('quote-summary').textContent=`币安 Bid/Ask ${n(q.binance.bid)} / ${n(q.binance.ask)}（原始 ${date(q.binance.source_time_ms||q.binance.time_ms)}） · MT5 Bid/Ask ${n(q.mt5.bid)} / ${n(q.mt5.ask)}（原始 ${date(q.mt5.source_time_ms||q.mt5.time_ms)}） · 入场（卖币安 Bid − 买 MT5 Ask） ${n(q.entry)} · 退出（买回币安 Ask − 卖 MT5 Bid） ${n(q.exit)} USD/盎司`;
      el('bid').value=q.binance.bid;el('binance-ask').value=q.binance.ask;
      el('mt5-bid').value=q.mt5.bid;el('mt5-ask').value=q.mt5.ask;
    }
  }
  function render(result) {
    last=result;
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
    el('trading-mode').textContent=result.connected ? `${mode} 已连接${positionLabel?' · '+positionLabel:''}` : '未连接';
    el('trading-mode').classList.toggle('warning', Boolean(state.alarm||result.last_error||(!reconciled&&result.connected)));
    const connectButton=el('trading-connect');
    connectButton.textContent=result.connected?'已连接双边行情（重新连接）':'连接双边行情';
    connectButton.classList.toggle('primary',!result.connected);
    streamLabel();
    const oldManaging=(state.groups||[]).some(g=>g.imported&&g.status!=='closed'&&g.management_enabled);
    const stateText = !result.connected ? '未连接，不能开仓' : (!reconciled || state.recovery) ? '已连接，等待持仓对账' : state.enabled ? '自动开仓运行中'+(oldManaging?' · 旧仓管理运行中':'') : oldManaging?'旧仓管理运行中 · 新开仓未启动':'已连接，尚未启动自动开仓';
    const alarm=state.alarm||result.last_error;
    const message=result.message ? `\n${result.message}` : '';
    const next=(!result.connected ? '请先连接行情。' : ((!reconciled||state.recovery) ? '请先点击“持仓对账”，对账通过后才能启动自动开仓。' : result.capabilities?.live_orders ? '实盘通道已连接、持仓已对账；启动时仍会检查配置和策略状态。' : '纸面模式不会发送真实订单。'));
    text('trading-result', `${actionError? actionError+'\n' : ''}${stateText}${alarm?'：'+alarm:''}${message}\n${next}\n${result.plan?`每组：MT5 ${result.plan.lots} 手 ↔ 币安 ${result.plan.qty} 盎司。`: '请保存参数后连接。'}`,Boolean(alarm||actionError));
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
    
    // 更新启动/暂停切换按钮
    const toggleButton = el('trading-toggle');
    if(state.enabled) {
      toggleButton.textContent = '暂停新开仓';
      toggleButton.classList.remove('primary');
    } else {
      toggleButton.textContent = '启动自动开仓';
      toggleButton.classList.add('primary');
    }
    
    showQuote(q);
    if(result.plan) {
      const p=result.plan, notional=q ? p.qty*q.binance.bid : null;
      text('plan-result',`MT5 ${el('mt5-symbol').value}：${n(p.lots)} 手 ↔ ${el('symbol').value}：${n(p.qty)} XAU\n黄金数量 ${n(p.qty)} 盎司；${notional===null?'等待行情':'当前名义金额约 '+Number(notional).toFixed(2)+' USDT（不是保证金）'}\n已按 MT5 与币安实际数量规则校验。`);
      el('plan-state').textContent='双边规则已校验';el('plan-state').classList.remove('warning');
    }
    renderIncome(q);
    el('orders').innerHTML=orderRows(state.orders||[],state.groups||[]);
    el('trade-events').textContent=(result.events||[]).slice(0,8).map(x=>`${new Date(x.time).toLocaleString()}  ${x.kind}  ${JSON.stringify(x.data)}`).join('\n');
    renderAdoption(result);
  }
  function renderIncome(q) {
    if(!last)return;
    const orders=last.state?.orders||[],raw=last.state?.groups||[];
    const groups=window.GoldPairQuotes?.markGroups?window.GoldPairQuotes.markGroups(raw,last.quote,q):raw;
    const active=groups.filter(g=>g.status!=='closed'),closed=groups.filter(g=>g.status==='closed');
    const sum=(items,key)=>items.reduce((total,g)=>total+Number(g.valuation?.[key]||0),0);
    const allCarry=sum(groups,'carry'),allFees=sum(groups,'fees')+sum(active,'estimated_exit_fee');
    el('pnl-stats').innerHTML=[['实时净收益',usd(sum(active,'net'))],['已平仓净收益',usd(sum(closed,'net'))],['资金费 + Swap',usd(allCarry)],['手续费及预估平仓费',`-${allFees.toFixed(2)} USD`]].map(([label,value])=>`<div class="stat"><span>${label}</span><strong>${value}</strong></div>`).join('');
    el('groups').innerHTML=groupRows(groups,orders);
    for(const button of el('groups').querySelectorAll('[data-close]')) button.onclick=()=>action(()=>closeOne(button.dataset.close));
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
    const generation=++plotGeneration;
    const minutes=Number(el('chart-window').value);
    const r=await request('/api/trading/chart?minutes='+minutes);
    if(generation!==plotGeneration)return;
    chartSamples=quoteBuffer?quoteBuffer.replaceHistory(r.samples||[],windowStart(),last?.quote?.key):(r.samples||[]);
    drawChart(chartSamples);
  }
  function drawChart(rawSamples) {
    const samples=window.GoldPairQuotes?window.GoldPairQuotes.renderPoints(rawSamples,Math.max(1,Number(el('chart-window').value)||1440)*60000,chartView):rawSamples;
    if(!chart) {
      chart=echarts.init(el('spread-chart'));
      if(chart.on)chart.on('datazoom',()=>{
        const zoom=chart.getOption()?.dataZoom?.[0];
        const first=chartSamples[0]?.time_ms||0,lastTime=chartSamples.at(-1)?.time_ms||first;
        chartView={start:first+(lastTime-first)*(zoom?.start||0)/100,end:first+(lastTime-first)*(zoom?.end??100)/100};
        paintSoon();
      });
    }
    const groups=last?.state?.groups||[];
    const marks=groups.filter(g=>g.status!=='closed').map(g=>({name:(g.imported?'旧仓 ':'')+'#'+g.id+(g.imported&&!g.management_enabled?' 目标（暂停）':' 止盈'),yAxis:g.parameters.target_mode==='absolute'?g.parameters.exit_spread_usd:g.entry-g.parameters.take_contraction_usd}));
    const expected=samples.length>1?(samples.at(-1).time_ms-samples[0].time_ms)/(samples.length-1):0;
    const lines=[];let previous;
    for(const x of samples){if(previous&&x.time_ms-previous.time_ms>Math.max(10000,expected*6,Math.max(1,Number(el('chart-window').value)||1440)*60000/3000))lines.push({time_ms:previous.time_ms+1,entry:null,exit:null});lines.push(x);previous=x;}
    const nearest=(at,field)=>{let best=null,distance=Infinity;for(const x of samples){const d=Math.abs(x.time_ms-at);if(d<distance){best=x;distance=d;}}return best&&Number.isFinite(best[field])?[at,best[field]]:null;};
    const firstTime=samples[0]?.time_ms??0,lastTime=samples.at(-1)?.time_ms??0;
    const opened=groups.filter(g=>!g.imported&&g.opened_ms>=firstTime&&g.opened_ms<=lastTime).map(g=>({name:'#'+g.id,value:[g.opened_ms,g.entry]}));
    const closed=groups.filter(g=>g.closed_ms>=firstTime&&g.closed_ms<=lastTime).map(g=>({name:'#'+g.id,value:[g.closed_ms,Number.isFinite(g.exit)?g.exit:nearest(g.closed_ms,'exit')?.[1]]})).filter(x=>Number.isFinite(x.value[1]));
    const entries=samples.map(x=>x.entry).filter(Number.isFinite), exits=samples.map(x=>x.exit).filter(Number.isFinite);
    const average=values=>values.length?values.reduce((a,b)=>a+b,0)/values.length:null;
    const latest=rawSamples.at(-1), all=entries.concat(exits);
    const entryThreshold=Number(el('entry').value), threshold=Number.isFinite(entryThreshold)?entryThreshold:null;
    el('spread-stats').innerHTML=[['当前入场',latest?.entry],['当前退出',latest?.exit],['开仓阈值',threshold],['窗口入场均值',average(entries)],['价差范围',all.length?`${Math.min(...all).toFixed(3)} ～ ${Math.max(...all).toFixed(3)}`:null]].map(([label,value])=>`<div class="stat"><span>${label}</span><strong>${typeof value==='number'?value.toFixed(3):value||'—'}</strong></div>`).join('');
    el('chart-empty').classList.toggle('is-hidden',samples.length>0);
    el('chart-coverage').textContent=samples.length?`本机可成交价差 ${samples.length.toLocaleString('zh-CN')} 个采样 · ${new Date(samples[0].time_ms).toLocaleString()} 至 ${new Date(samples.at(-1).time_ms).toLocaleString()} · 高频记录保留 24 小时，更早按分钟归档保留 30 天；历史保存在本机数据目录。`:'尚无本机历史；连接后开始记录真实 Bid/Ask 可成交价差，历史会保存到本机数据目录。';
    const oldZoom=chart.getOption()?.dataZoom?.[0];
    const start=resetChartZoom?0:(oldZoom?.start??0),end=resetChartZoom?100:(oldZoom?.end??100);resetChartZoom=false;
    const thresholdLine=threshold===null?[]:[{name:`开仓阈值 ${threshold.toFixed(2)}`,yAxis:threshold,lineStyle:{color:'#b75e24',width:2,type:'dashed'},label:{show:true,color:'#8a461e',formatter:`开仓阈值 ${threshold.toFixed(2)}`}}];
    chart.setOption({animation:false,legend:{top:5,type:'scroll'},tooltip:{trigger:'axis',confine:true,valueFormatter:v=>Number.isFinite(v)?v.toFixed(3):'—'},grid:{left:75,right:120,top:70,bottom:80},xAxis:{type:'time',minInterval:1000,axisPointer:{label:{formatter:params=>dateTime(params.value)}}},yAxis:{type:'value',scale:true,name:'价差 USD/盎司'},dataZoom:[{type:'inside',filterMode:'none',minValueSpan:1000,start,end},{type:'slider',filterMode:'none',minValueSpan:1000,bottom:15,start,end}],series:[
      {id:'entry',name:'入场（卖币安Bid / 买MT5Ask）',type:'line',showSymbol:false,connectNulls:false,data:lines.map(x=>[x.time_ms,x.entry]),itemStyle:{color:'#246b92'},lineStyle:{width:2,type:'solid'},markArea:{silent:true,itemStyle:{color:'rgba(36,107,146,.07)'},data:threshold===null?[]:[[{name:'开仓触发区',yAxis:threshold},{yAxis:'max'}]]},markLine:{symbol:'none',silent:true,data:[...thresholdLine,...marks],label:{formatter:'{b}: {c}'}}},
      {id:'exit',name:'退出（买回币安Ask / 卖MT5Bid）',type:'line',showSymbol:false,connectNulls:false,data:lines.map(x=>[x.time_ms,x.exit]),itemStyle:{color:'#247b68'},lineStyle:{width:2,type:'dashed'}},
      {id:'open',name:'策略开仓',type:'scatter',symbol:'triangle',symbolSize:12,itemStyle:{color:'#b75e24'},data:opened},
      {id:'close',name:'策略平仓',type:'scatter',symbol:'diamond',symbolSize:12,itemStyle:{color:'#6b50a6'},data:closed}
    ]},{notMerge:true,lazyUpdate:true});
  }
  async function status(withChart=true) {
    const r=await request('/api/trading/status');
    if(window.goldPairUiBusy) return;
    render(r);
    // Keep the last chart visible when disconnected, but do not keep
    // re-fetching it as if live monitoring were still running.
    if(withChart && (!chart || (!streamReady && r.connected))) await plot();
  }
  async function connect() {
    await saveConfig();
    const r=await request('/api/trading/connect',{api_key:el('trading-api-key').value.trim(),api_secret:el('trading-api-secret').value.trim(),mt5_mcp_token:el('mt5-mcp-token').value.trim()});
    render(r);await plot();
  }
  async function closeOne(group) { render(await request('/api/trading/close',{group,reason:'用户请求平仓'})); }
  el('trading-connect').onclick=()=>action(connect,'trading-connect','正在检查行情、账户和合约规则…');
  el('trading-reconcile').onclick=()=>action(async()=>{try {render(await request('/api/trading/reconcile',{}));await plot();} catch(error) {await status(false).catch(()=>{});throw error;}},'trading-reconcile','正在核对两边持仓…');
  el('trading-toggle').onclick=()=>action(async()=>{
    const state = last?.state;
    if(state?.enabled) {
      render(await request('/api/trading/pause',{}));
    } else {
      await saveConfig();
      render(await request('/api/trading/start',{}));
      await plot();
    }
  },'trading-toggle','正在切换自动开仓状态…');
  el('trading-close').onclick=()=>action(async()=>{render(await request('/api/trading/close',{reason:'用户请求全部平仓'}));},'trading-close','正在处理平仓请求…');
  el('chart-window').onchange=()=>{resetChartZoom=true;chartView=null;action(plot,'chart-window','正在加载图表数据…');};
  el('chart-latest').onclick=()=>{resetChartZoom=true;chartView=null;action(plot,'chart-latest','正在加载最新图表…');};
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
  window.addEventListener('beforeunload',()=>stream?.close());
  window.addEventListener('resize',()=>chart?.resize());
  setTimeout(()=>status().then(startStream).catch(error=>text('trading-result',error.message,true)),400);
  timer=setInterval(()=>{if(!busy&&!window.goldPairUiBusy)status().catch(error=>text('trading-result',error.message,true));},3000);
})();
