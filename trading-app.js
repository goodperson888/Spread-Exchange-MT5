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
    for(const id of ['trading-connect','trading-reconcile','trading-toggle','trading-close']) {
      el(id).disabled=value;
      el(id).classList.toggle('processing', value);
    }
    const active=activeId&&el(activeId);
    if(active?.tagName==='BUTTON') {
      if(value) { active.dataset.label=active.textContent; active.dataset.pendingLabel=label||'处理中…'; active.textContent=active.dataset.pendingLabel; }
      else if(active.textContent===active.dataset.pendingLabel) active.textContent=active.dataset.label||active.textContent;
    }
  }
  async function action(work, activeId, label) {
    if(busy)return; actionError=''; lock(true,activeId,label);
    text('trading-result', label||'正在处理，请稍候…');
    try { await work(); } catch(error) { actionError='本次操作未完成：'+error.message; text('trading-result',actionError,true); }
    finally { lock(false,activeId); }
  }
  function groupRows(groups, orders) {
    if(!groups?.length) return '<p class="note">暂无本策略持仓或交易记录。</p>';
    const states={opening:'开仓中',open:'持仓中',closing:'平仓中',unwinding:'异常撤回',attention:'需人工处理',closed:'已平仓'};
    const rows=groups.slice().reverse().map(g => {
      const v=g.valuation||{}, remaining=v.remaining||{};
      const own=orders.filter(o=>o.group===g.id&&o.action==='open'&&Number(o.result?.qty)>0);
      const amount=leg=>own.filter(o=>o.leg===leg).reduce((sum,o)=>sum+Number(o.result.qty)*Number(o.result.price),0);
      const basis=g.mode==='paper'?'纸面估算':v.costs_verified?'平台已复核':'实盘估算';
      const funding=Number(v.binance_funding||0),swap=Number(v.mt5_swap||0);
      const carryDetail=('binance_funding' in v||'mt5_swap' in v)?`资金费 ${usd(funding)}<br>MT5 Swap ${usd(swap)}`:`合计 ${usd(v.carry||0)}`;
      const action=g.status==='closed'?'':`<button data-close="${escape(g.id)}">平仓</button>`;
      const gridLabel=Number(g.grid_index||0)>0?`<br><span class="muted">网格补仓第 ${Number(g.grid_index)} 次</span>`:'';
      return `<tr><td><strong>#${escape(g.id)}</strong>${gridLabel}<br><span class="muted">${dateTime(g.opened_ms)}${g.closed_ms?'<br>→ '+dateTime(g.closed_ms):''}</span></td><td>${escape(states[g.status]||g.status)}<br><span class="record-basis">${basis}</span></td><td>${n(g.lots)} 手 / ${n(g.qty)} 盎司<br><span class="muted">币安 ${amount('binance').toFixed(2)} USDT<br>MT5 ${amount('mt5').toFixed(2)} USD</span></td><td>毛收益 ${usd(v.gross)}<br>手续费 -${Number(v.fees||0).toFixed(2)} USD${Number(v.estimated_exit_fee)>0?'<br>预估平仓费 -'+Number(v.estimated_exit_fee).toFixed(2)+' USD':''}</td><td>${carryDetail}</td><td class="${Number(v.net)>=0?'pnl-positive':'pnl-negative'}"><strong>${usd(v.net)}</strong><br><span class="muted">${g.status==='closed'?'平仓净收益':'实时净收益'}</span></td><td>币安 ${n(remaining.binance||0)}<br>MT5 ${n(remaining.mt5||0)}${g.reason?'<br>'+escape(g.reason):''}</td><td>${action}</td></tr>`;
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
      const status=r.status==='done'?(filled>0?'已成交':'未成交'):(r.status==='pending'?'待确认':'状态未知');
      const fee=Number.isFinite(Number(r.fee))?`${Number(r.fee).toFixed(4)} ${o.leg==='binance'?'USDT':'USD'}`:'未单独回填';
      return `<tr><td>${dateTime(o.created_ms)}</td><td>#${escape(o.group)}</td><td>${o.leg==='binance'?'币安':'MT5'}<br>${escape(o.symbol)}</td><td>${o.action==='open'?'开仓':'平仓'} · ${direction}</td><td>申请 ${n(o.requested)} 盎司<br>成交 ${quantity}</td><td>${price?`${n(price)}<br>${(filled*price).toFixed(2)} ${o.leg==='binance'?'USDT':'USD'}`:'—'}</td><td>${fee}</td><td>${status}${r.error?'<br>'+escape(r.error):''}<br><span class="muted">${escape(r.ticket||o.id)}</span></td></tr>`;
    }).join('');
    return `<table class="records-table"><thead><tr><th>时间</th><th>交易组</th><th>平台 / 品种</th><th>动作</th><th>申请 / 成交数量</th><th>成交价 / 金额</th><th>成交手续费</th><th>状态 / 票据</th></tr></thead><tbody>${rows}</tbody></table>`;
  }
  function render(result) {
    last=result;
    const state=result.state||{}, q=result.quote;
    const mode=result.capabilities?.mode||'paper';
    const reconciled=result.capabilities?.reconciled === true;
    const positionMode=result.capabilities?.position_mode;
    const positionLabel=positionMode==='hedge'?'双向持仓':positionMode==='one_way'?'单向持仓':'';
    el('trading-mode').textContent=result.connected ? `${mode} 已连接${positionLabel?' · '+positionLabel:''}` : '未连接';
    el('trading-mode').classList.toggle('warning', Boolean(state.alarm||result.last_error||(!reconciled&&result.connected)));
    const connectButton=el('trading-connect');
    connectButton.textContent=result.connected?'已连接双边行情（重新连接）':'连接双边行情';
    connectButton.classList.toggle('primary',!result.connected);
    const chartStatus=el('chart-live-status');
    if(chartStatus) {
      chartStatus.textContent=result.connected?'实时采样中 · 策略每 250 毫秒检查':'未连接 · 图表保留历史';
      chartStatus.classList.toggle('warning',!result.connected);
    }
    const stateText = !result.connected ? '未连接，不能开仓' : (!reconciled || state.recovery) ? '已连接，等待持仓对账' : state.enabled ? '自动开仓运行中' : '已连接，尚未启动自动开仓';
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
    
    if(q) {
      el('quote-summary').textContent=`币安 Bid/Ask ${n(q.binance.bid)} / ${n(q.binance.ask)}（${date(q.binance.time_ms)}） · MT5 Bid/Ask ${n(q.mt5.bid)} / ${n(q.mt5.ask)}（${date(q.mt5.time_ms)}） · 入场（卖币安 Bid − 买 MT5 Ask） ${n(q.entry)} · 退出（买回币安 Ask − 卖 MT5 Bid） ${n(q.exit)} USD/盎司`;
      el('bid').value=q.binance.bid;el('binance-ask').value=q.binance.ask;
      el('mt5-bid').value=q.mt5.bid;el('mt5-ask').value=q.mt5.ask;
    }
    if(result.plan) {
      const p=result.plan, notional=q ? p.qty*q.binance.bid : null;
      text('plan-result',`MT5 ${el('mt5-symbol').value}：${n(p.lots)} 手 ↔ ${el('symbol').value}：${n(p.qty)} XAU\n黄金数量 ${n(p.qty)} 盎司；${notional===null?'等待行情':'当前名义金额约 '+Number(notional).toFixed(2)+' USDT（不是保证金）'}\n已按 MT5 与币安实际数量规则校验。`);
      el('plan-state').textContent='双边规则已校验';el('plan-state').classList.remove('warning');
    }
    const groups=state.groups||[],orders=state.orders||[];
    const active=groups.filter(g=>g.status!=='closed'),closed=groups.filter(g=>g.status==='closed');
    const sum=(items,key)=>items.reduce((total,g)=>total+Number(g.valuation?.[key]||0),0);
    const allCarry=sum(groups,'carry'),allFees=sum(groups,'fees')+sum(active,'estimated_exit_fee');
    el('pnl-stats').innerHTML=[['实时净收益',usd(sum(active,'net'))],['已平仓净收益',usd(sum(closed,'net'))],['资金费 + Swap',usd(allCarry)],['手续费及预估平仓费',`-${allFees.toFixed(2)} USD`]].map(([label,value])=>`<div class="stat"><span>${label}</span><strong>${value}</strong></div>`).join('');
    el('groups').innerHTML=groupRows(groups,orders);
    el('orders').innerHTML=orderRows(orders,groups);
    for(const button of el('groups').querySelectorAll('[data-close]')) button.onclick=()=>action(()=>closeOne(button.dataset.close));
    el('trade-events').textContent=(result.events||[]).slice(0,8).map(x=>`${new Date(x.time).toLocaleString()}  ${x.kind}  ${JSON.stringify(x.data)}`).join('\n');
  }
  async function plot() {
    const minutes=Number(el('chart-window').value);
    const r=await request('/api/trading/chart?minutes='+minutes);
    const samples=r.samples||[];
    if(!chart) chart=echarts.init(el('spread-chart'));
    const groups=last?.state?.groups||[];
    const marks=groups.filter(g=>g.status!=='closed').map(g=>({name:'#'+g.id+' 止盈',yAxis:g.parameters.target_mode==='absolute'?g.parameters.exit_spread_usd:g.entry-g.parameters.take_contraction_usd}));
    const expected=samples.length>1?(samples.at(-1).time_ms-samples[0].time_ms)/(samples.length-1):0;
    const lines=[];let previous;
    for(const x of samples){if(previous&&x.time_ms-previous.time_ms>Math.max(10000,expected*6))lines.push({time_ms:previous.time_ms+1,entry:null,exit:null});lines.push(x);previous=x;}
    const nearest=(at,field)=>{let best=null,distance=Infinity;for(const x of samples){const d=Math.abs(x.time_ms-at);if(d<distance){best=x;distance=d;}}return best&&Number.isFinite(best[field])?[at,best[field]]:null;};
    const firstTime=samples[0]?.time_ms??0,lastTime=samples.at(-1)?.time_ms??0;
    const opened=groups.filter(g=>g.opened_ms>=firstTime&&g.opened_ms<=lastTime).map(g=>({name:'#'+g.id,value:[g.opened_ms,g.entry]}));
    const closed=groups.filter(g=>g.closed_ms>=firstTime&&g.closed_ms<=lastTime).map(g=>({name:'#'+g.id,value:[g.closed_ms,Number.isFinite(g.exit)?g.exit:nearest(g.closed_ms,'exit')?.[1]]})).filter(x=>Number.isFinite(x.value[1]));
    const entries=samples.map(x=>x.entry).filter(Number.isFinite), exits=samples.map(x=>x.exit).filter(Number.isFinite);
    const average=values=>values.length?values.reduce((a,b)=>a+b,0)/values.length:null;
    const latest=samples.at(-1), all=entries.concat(exits);
    const entryThreshold=Number(el('entry').value), threshold=Number.isFinite(entryThreshold)?entryThreshold:null;
    el('spread-stats').innerHTML=[['当前入场',latest?.entry],['当前退出',latest?.exit],['开仓阈值',threshold],['窗口入场均值',average(entries)],['价差范围',all.length?`${Math.min(...all).toFixed(3)} ～ ${Math.max(...all).toFixed(3)}`:null]].map(([label,value])=>`<div class="stat"><span>${label}</span><strong>${typeof value==='number'?value.toFixed(3):value||'—'}</strong></div>`).join('');
    el('chart-empty').classList.toggle('is-hidden',samples.length>0);
    el('chart-coverage').textContent=samples.length?`本机可成交价差 ${samples.length.toLocaleString('zh-CN')} 个采样 · ${new Date(samples[0].time_ms).toLocaleString()} 至 ${new Date(samples.at(-1).time_ms).toLocaleString()} · 高频记录保留 24 小时，更早按分钟归档保留 30 天；历史保存在本机数据目录。`:'尚无本机历史；连接后开始记录真实 Bid/Ask 可成交价差，历史会保存到本机数据目录。';
    const oldZoom=chart.getOption()?.dataZoom?.[0];
    const start=resetChartZoom?0:(oldZoom?.start??0),end=resetChartZoom?100:(oldZoom?.end??100);resetChartZoom=false;
    const thresholdLine=threshold===null?[]:[{name:`开仓阈值 ${threshold.toFixed(2)}`,yAxis:threshold,lineStyle:{color:'#b75e24',width:2,type:'dashed'},label:{show:true,color:'#8a461e',formatter:`开仓阈值 ${threshold.toFixed(2)}`}}];
    chart.setOption({animation:false,legend:{top:5,type:'scroll'},tooltip:{trigger:'axis',confine:true,valueFormatter:v=>Number.isFinite(v)?v.toFixed(3):'—'},grid:{left:75,right:120,top:70,bottom:80},xAxis:{type:'time',minInterval:1000,axisPointer:{label:{formatter:params=>dateTime(params.value)}}},yAxis:{type:'value',scale:true,name:'价差 USD/盎司'},dataZoom:[{type:'inside',filterMode:'none',minValueSpan:1000,start,end},{type:'slider',filterMode:'none',minValueSpan:1000,bottom:15,start,end}],series:[
      {id:'entry',name:'入场（卖币安Bid / 买MT5Ask）',type:'line',showSymbol:false,sampling:'lttb',connectNulls:false,data:lines.map(x=>[x.time_ms,x.entry]),itemStyle:{color:'#246b92'},lineStyle:{width:2,type:'solid'},markArea:{silent:true,itemStyle:{color:'rgba(36,107,146,.07)'},data:threshold===null?[]:[[{name:'开仓触发区',yAxis:threshold},{yAxis:'max'}]]},markLine:{symbol:'none',silent:true,data:[...thresholdLine,...marks],label:{formatter:'{b}: {c}'}}},
      {id:'exit',name:'退出（买回币安Ask / 卖MT5Bid）',type:'line',showSymbol:false,sampling:'lttb',connectNulls:false,data:lines.map(x=>[x.time_ms,x.exit]),itemStyle:{color:'#247b68'},lineStyle:{width:2,type:'dashed'}},
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
    if(withChart && (r.connected || !chart)) await plot();
  }
  async function connect() {
    await saveConfig();
    const r=await request('/api/trading/connect',{api_key:el('trading-api-key').value.trim(),api_secret:el('trading-api-secret').value.trim(),mt5_mcp_token:el('mt5-mcp-token').value.trim()});
    render(r);await plot();
  }
  async function closeOne(group) { render(await request('/api/trading/close',{group,reason:'用户请求平仓'})); }
  el('trading-connect').onclick=()=>action(connect,'trading-connect','正在检查行情、账户和合约规则…');
  el('trading-reconcile').onclick=()=>action(async()=>{render(await request('/api/trading/reconcile'));await plot();},'trading-reconcile','正在核对两边持仓…');
  el('trading-toggle').onclick=()=>action(async()=>{
    const state = last?.state;
    if(state?.enabled) {
      render(await request('/api/trading/pause'));
    } else {
      await saveConfig();
      render(await request('/api/trading/start'));
      await plot();
    }
  },'trading-toggle','正在切换自动开仓状态…');
  el('trading-close').onclick=()=>action(async()=>{render(await request('/api/trading/close',{reason:'用户请求全部平仓'}));},'trading-close','正在处理平仓请求…');
  el('chart-window').onchange=()=>{resetChartZoom=true;action(plot,'chart-window','正在加载图表数据…');};
  el('chart-latest').onclick=()=>{resetChartZoom=true;action(plot,'chart-latest','正在加载最新图表…');};
  window.addEventListener('resize',()=>chart?.resize());
  setTimeout(()=>status().catch(error=>text('trading-result',error.message,true)),400);
  timer=setInterval(()=>{if(!busy&&!window.goldPairUiBusy)status().catch(error=>text('trading-result',error.message,true));},3000);
})();
