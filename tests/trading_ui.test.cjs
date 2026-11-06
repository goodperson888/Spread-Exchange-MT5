const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

// Execute the actual button handlers without contacting any trading account.
function pageFixture(reconcileFails = false, handlers = {}) {
  const elements = new Map(), calls = [], charts=[], streams=[], listeners={}, windowListeners={};
  const intervals=[];let testNow=Date.now();
  class TestDate extends Date {static now(){return testNow;}}
  const doc={hidden:false, getElementById:id=>el(id),addEventListener:(name,fn)=>{listeners[name]=fn;}};
  class FakeStream {
    constructor(){this.handlers={};this.closed=false;streams.push(this);}
    addEventListener(name,fn){this.handlers[name]=fn;}
    close(){this.closed=true;}
    emit(payload){this.handlers.quotes?.({data:JSON.stringify(payload)});}
  }
  const el = id => {
    if (!elements.has(id)) elements.set(id, {
      tagName:'BUTTON', value:'', textContent:'', innerHTML:'', dataset:{},
      classList:{toggle(){}, add(){}, remove(){}}, querySelectorAll(selector){
        if(id==='groups'&&selector==='[data-edit-exit]'){
          if(this.editButtonsHTML!==this.innerHTML){
            this.editButtonsHTML=this.innerHTML;
            this.editButtons=[...this.innerHTML.matchAll(/<button data-edit-exit="([^"]+)"([^>]*)>/g)]
              .map(m=>({dataset:{editExit:m[1]},disabled:m[2].includes('disabled')}));
          }
          return this.editButtons||[];
        }
        if(id!=='groups'||selector!=='[data-close]')return [];
        if(this.buttonsHTML!==this.innerHTML){
          this.buttonsHTML=this.innerHTML;
          this.buttons=[...this.innerHTML.matchAll(/<button data-close="([^"]+)"([^>]*)>/g)]
            .map(m=>({dataset:{close:m[1]},disabled:m[2].includes('disabled')}));
        }
        return this.buttons||[];
      }, addEventListener(name,fn){this['on'+name]=fn;}
    });
    return elements.get(id);
  };
  let enabled = false, reconciled = false;
  const snapshot = () => ({connected:true, state:{enabled, groups:[], orders:[]},
    capabilities:{mode:'live', reconciled}});
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../trading-app.js'), 'utf8'), {
    document:doc, window:{addEventListener(name,fn){windowListeners[name]=fn;},EventSource:FakeStream,GoldPairQuotes:require('../quote-stream.js')},
    loaded:false, saveConfig:async()=>{}, AbortController,
    Date:TestDate,setTimeout(){}, clearTimeout(){}, setInterval(fn,ms){intervals.push({fn,ms});},
    echarts:{init:()=>({getOption:()=>({}), resize(){},setOption(option){charts.push(option);}})},
    fetch:async (url, options) => {
      calls.push({url, ...options});
      const action = url.split('/').at(-1);
      let body = {}, status = 200;
      if (handlers[url]) body=await handlers[url](options.body===undefined?undefined:JSON.parse(options.body));
      else if (['reconcile','start','pause'].includes(action)) {
        if (options.method !== 'POST') {status=404; body={error:'未找到'};}
        else if (action==='reconcile' && reconcileFails) {status=400; body={error:'持仓不一致'};}
        else {
          if (action==='reconcile') reconciled=true;
          if (action==='start') enabled=true;
          if (action==='pause') enabled=false;
          body=snapshot();
        }
      } else if (action==='status') body=snapshot();
      else if (action.startsWith('chart?')) body={samples:[]};
      else throw Error('Unexpected endpoint: '+url);
      return {ok:status===200, json:async()=>body};
    }
  });
  return {el, calls, charts, streams, doc, listeners,windowListeners,
    ageRisk(ms){testNow+=ms;for(const interval of intervals)if(interval.ms===1000)interval.fn();}};
}

test('risk cards show both platforms and expire even when status polling stops',async()=>{
  const f=pageFixture(false,{'/api/trading/reconcile':()=>({connected:true,state:{groups:[],orders:[]},
    liquidation_risk:{binance:{level:'warning',message:'接近强平',age_ms:1000,stale_after_ms:20000,
      distance_percent:3,equity:900,maintenance_margin:30,positions:[{side:'short',qty:1,mark_price:4000,liquidation_price:4120,distance_percent:3}]},
      mt5:{level:'danger',message:'保证金不足',age_ms:1000,margin:100,equity:80,margin_level:80,
        currency:'USD',margin_so_mode:0,margin_so_call:100,margin_so_so:50}}})});
  await f.el('trading-reconcile').onclick();
  assert.equal(f.el('risk-binance-card').dataset.level,'warning');
  assert.equal(f.el('risk-mt5-card').dataset.level,'danger');
  assert.match(f.el('risk-binance-metrics').innerHTML,/空头 1 XAU.*强平价 4,120/);
  assert.match(f.el('risk-mt5-metrics').innerHTML,/经纪商强平线.*50%/);
  const calls=f.calls.length;
  f.ageRisk(20000);
  assert.equal(f.el('risk-binance-card').dataset.level,'stale');
  assert.equal(f.el('risk-mt5-card').dataset.level,'stale');
  assert.match(f.el('risk-binance-state').textContent,/数据过期/);
  assert.match(f.el('risk-binance-message').textContent,/当前风险状态未知/);
  assert.equal(f.calls.length,calls);
});

test('unknown risk does not render missing metrics as zero and escapes upstream text',async()=>{
  const f=pageFixture(false,{'/api/trading/reconcile':()=>({state:{groups:[],orders:[]},
    liquidation_risk:{binance:{level:'unknown',equity:null,positions:[]},
      mt5:{level:'unknown',currency:'<img src=x>',margin_so_mode:1,margin_so_so:50}}})});
  await f.el('trading-reconcile').onclick();
  assert.equal(f.el('risk-binance-card').dataset.level,'unknown');
  assert.doesNotMatch(f.el('risk-binance-metrics').innerHTML,/>0 USDT/);
  assert.match(f.el('risk-mt5-metrics').innerHTML,/&lt;img src=x&gt;/);
  assert.doesNotMatch(f.el('risk-mt5-metrics').innerHTML,/<img/);
});

test('event log translates preflight reasons, merges duplicates and keeps more than eight rows', async () => {
  const events=[{time:30000,kind:'entry_preflight_blocked',data:{threshold:3.7,reason:'校时失败'}},
    {time:29999,kind:'entry_preflight_blocked',data:{threshold:3.7,reason:'校时失败'}},
    ...Array.from({length:12},(_,i)=>({time:20000-i*1000,kind:'entry_threshold_applied',data:{value:i}})),
    {time:1000,kind:'entry_preflight_blocked',data:{threshold:3.7}}];
  const {el}=pageFixture(false,{'/api/trading/reconcile':()=>({state:{groups:[],orders:[]},events})});
  el('trade-events').scrollTop=150;
  await el('trading-reconcile').onclick();
  const content=el('trade-events').textContent;
  assert.match(content,/开仓预检暂未通过（合并 2 条）/);
  assert.match(content,/开仓阈值 3.7 USD\/盎司；校时失败/);
  assert.match(content,/旧日志未记录具体原因/);
  assert.match(content,/数值：11/);
  assert.doesNotMatch(content,/entry_preflight_blocked|"threshold"/);
  assert.equal(el('trade-events').scrollTop,150);
});

test('reconcile, start and pause buttons send POST; chart remains GET', async () => {
  const {el, calls} = pageFixture();
  await el('trading-reconcile').onclick();
  assert.doesNotMatch(el('trading-result').textContent, /本次操作未完成/);
  await el('trading-toggle').onclick();
  assert.equal(el('trading-toggle').disabled,true);
  assert.equal(el('trading-pause').disabled,false);
  assert.match(el('new-entry-status').textContent,/运行中/);
  await el('trading-pause').onclick();
  assert.equal(el('trading-toggle').textContent, '启动自动开仓');
  assert.equal(el('trading-pause').disabled,true);
  assert.match(el('new-entry-status').textContent,/已暂停/);
  const actions=calls.filter(x=>!x.url.includes('chart?'));
  assert.deepEqual(actions.map(x=>x.url), ['reconcile','start','pause'].map(x=>'/api/trading/'+x));
  for (const call of actions) {
    assert.equal(call.method, 'POST');
    assert.deepEqual(JSON.parse(call.body), {});
    assert.equal(call.headers['X-Local-App'], 'GoldPairLocal');
  }
  assert.ok(calls.some(x=>x.url.includes('chart?') && x.method==='GET'));
});

test('entry threshold can be applied while connected without reconnecting', async () => {
  const {el, calls} = pageFixture(false, {
    '/api/trading/apply-entry': body => {
      assert.equal(body.entry_spread_usd, '3.5');
      return {connected:true, state:{enabled:true, groups:[], orders:[]}, capabilities:{mode:'live', reconciled:true}, strategy_runtime:{entry_spread_usd:3.5}};
    }
  });
  el('entry').value='3.5';
  await el('apply-entry').onclick();
  const call=calls.find(x=>x.url==='/api/trading/apply-entry');
  assert.equal(call.method,'POST');
  assert.equal(el('entry-effective').textContent,'当前生效：3.5（已有交易组不变）');
  assert.equal(calls.some(x=>x.url==='/api/trading/connect'),false);
});

test('failed reconcile refreshes status and preserves the actionable error', async () => {
  const {el, calls} = pageFixture(true);
  await el('trading-reconcile').onclick();
  assert.match(el('trading-result').textContent, /持仓不一致/);
  assert.deepEqual(calls.map(x=>[x.url,x.method]), [
    ['/api/trading/reconcile','POST'], ['/api/trading/status','GET']
  ]);
  assert.equal(el('trading-reconcile').disabled, false);
});

test('adoption buttons send explicit POST, confirmation stays paused, management is separate', async () => {
  const group={id:'old-basket',imported:true,management_enabled:false,status:'open',mode:'live',
    entry:6,qty:1,lots:.01,opened_ms:1500,parameters:{take_contraction_usd:2,min_net_profit_usd:0}};
  const status=()=>({state:{enabled:false,groups:[group],orders:[]}});
  const {el,calls,charts}=pageFixture(false,{
    '/api/trading/chart?minutes=0':()=>({samples:[{time_ms:1000,entry:6,exit:7},{time_ms:2000,entry:5,exit:6}]}),
    '/api/trading/adoption/preview':body=>{
      assert.deepEqual(body.tickets,['123']);assert.equal(body.entry_fx,'1');
      return {preview:{...group,positions:[{ticket:'123'}],entry_fx:1,binance_entry:4306,mt5_entry:4300,
        history_fees_usd:0,history_funding_usd:0}};
    },
    '/api/trading/adoption/confirm':body=>{assert.equal(body.preview_id,group.id);return status();},
    '/api/trading/adoption/manage':body=>{
      assert.equal(body.group,group.id);group.management_enabled=body.enabled;return status();
    }
  });
  el('adoption-tickets').querySelectorAll=()=>[{value:'123'}];
  el('adoption-fx').value='1';el('adoption-costs-confirmed').checked=true;
  await el('adoption-preview').onclick();
  assert.equal(el('adoption-confirm').disabled,false);
  await el('adoption-confirm').onclick();
  assert.equal(el('adoption-manage').textContent,'启动已有仓位管理');
  await el('adoption-manage').onclick();
  assert.equal(el('adoption-manage').textContent,'暂停已有仓位管理');
  await el('adoption-manage').onclick();
  assert.equal(el('adoption-manage').textContent,'启动已有仓位管理');
  assert.ok(calls.every(c=>c.method==='POST' && c.url.startsWith('/api/trading/adoption/')));
  await el('chart-latest').onclick();
  await new Promise(setImmediate);
  assert.doesNotMatch(el('trading-result').textContent,/本次操作未完成/);
  assert.equal(charts.at(-1).series.find(s=>s.id==='open').data.length,0);
  assert.ok(charts.at(-1).series[0].markLine.data.some(x=>x.name.includes('旧仓')&&x.name.includes('暂停')));
});


test('hidden page disconnects quotes; resume loads history once and never sends trading commands',async()=>{
  const {doc,listeners,streams,calls,charts}=pageFixture();
  const flush=()=>new Promise(resolve=>setImmediate(resolve));
  listeners.visibilitychange();await flush();
  assert.equal(streams.length,1);
  streams[0].emit({reset:true,samples:[]});await flush();
  const initial=calls.length;
  doc.hidden=true;listeners.visibilitychange();
  assert.equal(streams[0].closed,true);
  streams[0].emit({reset:true,samples:[]});await flush();
  assert.equal(calls.length,initial);
  doc.hidden=false;listeners.visibilitychange();await flush();
  assert.equal(streams.length,2);
  streams[0].emit({reset:true,samples:[]});
  streams[1].emit({reset:true,samples:[]});await flush();
  assert.equal(calls.filter(c=>c.url.includes('/chart?')).length,2);
  assert.ok(calls.every(c=>c.method==='GET'));
  assert.equal(charts.length,2);
});

test('hidden page cancels outstanding historical request and ignores late response',async()=>{
  let release,signal;
  const f=pageFixture(false,{'/api/trading/chart?minutes=0':()=>new Promise(r=>{release=r;})});
  const work=f.el('chart-latest').onclick();
  signal=f.calls.find(c=>c.url.includes('/chart?')).signal;
  f.doc.hidden=true;f.listeners.visibilitychange();
  assert.equal(signal.aborted,true);
  release({samples:[]});await work;
  assert.equal(f.charts.length,0);
});

test('chart keeps backend threshold while input is pending; no zero threshold before connection',async()=>{
  let connected=false;
  const f=pageFixture(false,{'/api/trading/reconcile':()=>({connected,state:{groups:[],orders:[]},strategy_runtime:connected?{entry_spread_usd:3.5}:null})});
  f.el('entry').value='9';
  await f.el('trading-reconcile').onclick();
  assert.equal(f.charts.at(-1).series[0].markLine.data.length,0);
  connected=true;
  await f.el('trading-reconcile').onclick();
  assert.equal(f.charts.at(-1).series[0].markLine.data[0].yAxis,3.5);
  assert.match(f.el('entry-effective').textContent,/当前生效：3.5 · 待应用：9/);
  f.el('entry').value='8';f.el('entry').oninput();
  assert.match(f.el('entry-effective').textContent,/待应用：8/);
});

test('unwound group and cumulative carry are explicit; rejected order never claims confirmed fill',async()=>{
  const g={id:'failed-pair',status:'closed',reason:'币安拒单、部分成交或状态未知',qty:1,lots:.01,contract:100,valuation:{net:-.47,carry:0}};
  const order={id:'rejected',group:g.id,leg:'binance',action:'open',requested:1,created_ms:1000,result:{status:'done',qty:0,price:0,error:'币安接口错误 -1021'}};
  const f=pageFixture(false,{'/api/trading/reconcile':()=>({state:{groups:[g],orders:[order]}})});
  await f.el('trading-reconcile').onclick();
  assert.match(f.el('groups').innerHTML,/异常撤回已完成/);
  assert.match(f.el('pnl-stats').innerHTML,/累计资金费 \+ Swap（含已平仓）/);
  assert.doesNotMatch(f.el('orders').innerHTML,/已通过对账确认|查询确认|发送→回报/);
});

test('execution timing names local stages and missing paired spread stays unknown',async()=>{
  const order={id:'one',group:'g',leg:'binance',action:'close',created_ms:1000,dispatch_ms:1010,
    signal_time_ms:990,intent_journal_ms:7,actual_spread:null,signal_spread:null,spread_slippage:null,
    result:{status:'done',qty:1,price:4300,request_ms:40,response_received_ms:1050,reconcile_time_ms:2000}};
  const f=pageFixture(false,{'/api/trading/reconcile':()=>({state:{groups:[],orders:[order]}})});
  await f.el('trading-reconcile').onclick();
  const html=f.el('orders').innerHTML;
  assert.match(html,/采样→调用 20 ms/);
  assert.match(html,/意图落盘 7 ms/);
  assert.match(html,/适配器调用 40 ms/);
  assert.match(html,/非实际成交延迟/);
  assert.match(html,/等待双边成交/);
  assert.doesNotMatch(html,/偏移 \+0/);
});


test('unfinished groups and unknown fills disable start even if connection was reconciled', async()=>{
  for(const status of ['opening','unwinding','closing','attention']) {
    const fixture=pageFixture(false,{'/api/trading/reconcile':()=>({connected:true,
      capabilities:{reconciled:true},state:{enabled:false,groups:[{id:'g1',status,parameters:{}}],orders:[]}})});
    await fixture.el('trading-reconcile').onclick();
    assert.equal(fixture.el('trading-toggle').disabled,true);
    assert.match(fixture.el('workflow-status').textContent,/尚未完成/);
  }
});

function unknownSnapshot(status='unwinding') {
  return {connected:true,capabilities:{reconciled:true},state:{enabled:false,groups:[{
    id:'g1',status,qty:1,lots:.01,contract:100,parameters:{},valuation:{net:3.04,remaining:{binance:1,mt5:0}}}],
    orders:[{id:'m1',group:'g1',leg:'mt5',action:'open',requested:1,result:{status:'unknown',qty:0,price:0}}]}};
}

test('unknown exposure and income are labelled as unconfirmed on both views', async()=>{
  const f=pageFixture(false,{'/api/trading/reconcile':()=>unknownSnapshot()});
  await f.el('trading-reconcile').onclick();
  assert.match(f.el('groups').innerHTML,/MT5 待确认（已确认 0）/);
  assert.match(f.el('groups').innerHTML,/<strong>—<\/strong>.*收益待核对/);
  assert.match(f.el('pnl-stats').innerHTML,/收益待核对/);
  assert.match(f.el('orders').innerHTML,/成交待确认/);
  assert.match(f.el('live-exposure').textContent,/成交待确认/);
  assert.equal(f.el('records-feedback').textContent,f.el('trading-result').textContent);
});

test('record close button shows pending response in the records view', async()=>{
  const f=pageFixture(false,{'/api/trading/reconcile':()=>unknownSnapshot(),
    '/api/trading/close':body=>{assert.equal(body.group,'g1');return {...unknownSnapshot('closing'),message:'平仓请求已记录；尚未双平'};}});
  await f.el('trading-reconcile').onclick();
  await f.el('groups').querySelectorAll('[data-close]')[0].onclick();
  assert.match(f.el('records-feedback').textContent,/平仓请求已记录；尚未双平/);
  assert.match(f.el('groups').innerHTML,/核对成交后继续平仓/);
});

test('record close errors remain visible where the user pressed the button', async()=>{
  const f=pageFixture(false,{'/api/trading/reconcile':()=>unknownSnapshot(),
    '/api/trading/close':()=>{throw Error('请先完成持仓对账');}});
  await f.el('trading-reconcile').onclick();
  await f.el('groups').querySelectorAll('[data-close]')[0].onclick();
  assert.match(f.el('records-feedback').textContent,/本次操作未完成：请先完成持仓对账/);
});

const flushUI=()=>new Promise(resolve=>setImmediate(resolve));
function monitoredSnapshot(now=Date.now()) {
  return {connected:true,capabilities:{mode:'live',reconciled:true},
    strategy_runtime:{entry_spread_usd:2.7,max_quote_age_ms:3000},
    position_report:{time_ms:now-10000,mt5:[],binance:[],binance_orders:[]},
    quote:{key:'a',time_ms:now,entry:3,exit:3.1,valid:true,
      mt5:{observed_ms:now,bid:100,ask:100.1},binance:{observed_ms:now,bid:103,ask:103.1}},
    state:{groups:[],orders:[]}};
}

test('returning to the trading view reloads a reset missed while records were visible',async()=>{
 const snapshot=monitoredSnapshot();
 const f=pageFixture(false,{'/api/trading/reconcile':()=>snapshot,'/api/trading/status':()=>snapshot});
 await f.el('trading-reconcile').onclick();
 const initial=f.calls.filter(c=>c.url.includes('chart?')).length;
 assert.ok(f.charts.length);
 f.listeners.visibilitychange();await flushUI();
 f.el('view-trading').hidden=true;
 f.streams.at(-1).emit({reset:true,samples:[snapshot.quote]});await flushUI();
 assert.equal(f.calls.filter(c=>c.url.includes('chart?')).length,initial);
 f.el('view-trading').hidden=false;
 f.windowListeners['gold-workspace-changed']();await flushUI();
 assert.equal(f.calls.filter(c=>c.url.includes('chart?')).length,initial+1);
 assert.equal(f.calls.filter(c=>c.method==='POST').length,1);
});

test('live ticks do not repeatedly abort a history recovery already in flight',async()=>{
 const snapshot=monitoredSnapshot();let finishHistory,hold=false;
 const f=pageFixture(false,{'/api/trading/reconcile':()=>snapshot,'/api/trading/status':()=>snapshot,
   '/api/trading/chart?minutes=0':()=>hold?new Promise(resolve=>{finishHistory=resolve;}):{samples:[]}});
 await f.el('trading-reconcile').onclick();
 f.listeners.visibilitychange();await flushUI();
 hold=true;
 const before=f.calls.filter(c=>c.url.includes('chart?')).length;
 f.streams.at(-1).emit({reset:true,samples:[snapshot.quote]});await flushUI();
 for(let i=1;i<=5;i++)f.streams.at(-1).emit({samples:[{...snapshot.quote,time_ms:snapshot.quote.time_ms+i}]});
 f.streams.at(-1).emit({reset:true,samples:[snapshot.quote]});
 await flushUI();
 assert.equal(f.calls.filter(c=>c.url.includes('chart?')).length,before+1);
 finishHistory({samples:[]});await flushUI();
 assert.doesNotMatch(f.el('trading-result').textContent,/恢复历史失败/);
 hold=false;f.windowListeners['gold-workspace-changed']();await flushUI();
 assert.equal(f.calls.filter(c=>c.url.includes('chart?')).length,before+2);
});

test('reconciliation freshness expires locally and distinguishes valuation from platform checks',async()=>{
 const snapshot=monitoredSnapshot();
 const f=pageFixture(false,{'/api/trading/reconcile':()=>snapshot});
 await f.el('trading-reconcile').onclick();
 assert.match(f.el('records-position-check').textContent,/上次完整对账.*10 秒前.*非持续进行/);
 const calls=f.calls.length;f.ageRisk(5000);
 assert.match(f.el('records-position-check').textContent,/报价已过期.*15 秒前/);
 assert.equal(f.calls.length,calls);
 assert.equal(f.el('position-check-status').textContent,f.el('records-position-check').textContent);
});

test('group rules stay visible when pending main parameters change and abnormal groups have no ordinary TP line',async()=>{
 const snapshot=monitoredSnapshot();snapshot.config_pending=true;
 snapshot.state.groups=['open','unwinding','closing'].map((status,i)=>({id:'g'+i,status,
   entry:3,lots:.01,qty:1,contract:100,parameters:{take_contraction_usd:2.3,max_slippage_usd:.2,unwind_slippage_usd:1},
   valuation:{remaining:{mt5:1,binance:1}}}));
 const f=pageFixture(false,{'/api/trading/reconcile':()=>snapshot});
 f.el('take-contraction').value='9';
 await f.el('trading-reconcile').onclick();
 assert.match(f.el('parameter-scope').textContent,/尚未全部生效.*现有组保留原规则/);
 assert.match(f.el('groups').innerHTML,/本组已锁定规则.*收窄 ≥ 2.3/);
 const marks=f.charts.at(-1).series[0].markLine.data;
 assert.ok(marks.some(x=>x.name.includes('#g0 已锁定止盈')));
 assert.ok(!marks.some(x=>/#g1|#g2/.test(x.name)));
});

test('chart shades missing samples and disconnects invalid quotes',async()=>{
 const now=Date.now(),snapshot=monitoredSnapshot(now);
 const rows=[now-500000,now-499000,now-498000,now-2000,now-1000,now]
   .map(time_ms=>({...snapshot.quote,time_ms}));
 rows.at(-1).valid=false;
 const f=pageFixture(false,{'/api/trading/reconcile':()=>snapshot,
   '/api/trading/chart?minutes=0':()=>({samples:rows})});
 await f.el('trading-reconcile').onclick();
 const line=f.charts.at(-1).series[0];
 assert.equal(line.connectNulls,false);
 assert.equal(line.data.filter(x=>x[1]===null).length,2);
 assert.match(f.el('chart-gaps').textContent,/2 处.*灰色/);
 assert.ok(line.markArea.data.some(x=>x[0].name==='本机采样缺失'));
 assert.ok(line.markArea.data.some(x=>x[0].name==='报价无效或过期'));
});

function editableSnapshot(mode='group') {
 const snapshot=monitoredSnapshot();
 snapshot.state.groups=[{id:'g1',key:'a',mode:'live',status:'open',entry:5.7,lots:.01,qty:1,contract:100,
  parameters:{...JSON.parse(fs.readFileSync(path.join(__dirname,'../config.example.json'),'utf8')).strategy,exit_mode:mode},
  valuation:{fees:.3,remaining:{mt5:1,binance:1}}}];
 return snapshot;
}

test('existing group exits require a preview and explicit apply without saving main parameters',async()=>{
 const snapshot=editableSnapshot();let applied;
 const f=pageFixture(false,{'/api/trading/reconcile':()=>snapshot,'/api/trading/exit-rules':body=>{
   applied=body;return {...snapshot,message:'已更新所选已有组',state:{...snapshot.state,
     groups:[{...snapshot.state.groups[0],exit_rules_revision:1,parameters:{...snapshot.state.groups[0].parameters,...body.rules}}]}};
 }});
 await f.el('trading-reconcile').onclick();
 f.el('groups').querySelectorAll('[data-edit-exit]')[0].onclick();
 assert.equal(f.el('exit-rules-panel').open,true);
 assert.equal(f.el('exit-rule-group').value,'group:g1');
 assert.equal(f.el('exit-rule-take').value,'2');
 f.el('exit-rule-take').value='1.5';f.el('exit-rule-fields').oninput();
 assert.equal(f.el('exit-rule-apply').disabled,true);
 f.el('exit-rule-preview').onclick();
 assert.match(f.el('exit-rule-result').textContent,/#g1 原规则.*收窄 ≥ 2[\s\S]*修改为.*收窄 ≥ 1.5[\s\S]*可能立即触发平仓/);
 assert.equal(f.calls.filter(c=>c.method==='POST').length,1);
 assert.equal(f.el('exit-rule-apply').disabled,false);
 await f.el('exit-rule-apply').onclick();
 assert.deepEqual(applied.groups,['g1']);assert.deepEqual(applied.expected_revisions,{g1:0});
 assert.equal(applied.rules.take_contraction_usd,1.5);
 assert.equal(f.el('exit-rule-apply').disabled,true);
 assert.equal(f.el('exit-rule-result').textContent,'已更新所选已有组');
 assert.ok(!f.calls.some(c=>c.url==='/api/config'||c.url.includes('/close')));
});

test('editing after preview and a newer server rule version both invalidate apply',async()=>{
 let snapshot=editableSnapshot();
 const f=pageFixture(false,{'/api/trading/reconcile':()=>snapshot,'/api/trading/status':()=>snapshot});
 await f.el('trading-reconcile').onclick();
 f.el('exit-rule-group').value='group:g1';f.el('exit-rule-load').onclick();
 f.el('exit-rule-preview').onclick();assert.equal(f.el('exit-rule-apply').disabled,false);
 f.el('exit-rule-take').value='1';f.el('exit-rule-fields').oninput();assert.equal(f.el('exit-rule-apply').disabled,true);
 f.el('exit-rule-preview').onclick();
 snapshot={...snapshot,state:{...snapshot.state,groups:[{...snapshot.state.groups[0],exit_rules_revision:1}]}};
 await f.windowListeners['gold-config-saved']();
 assert.equal(f.el('exit-rule-apply').disabled,true);
 assert.equal(f.el('exit-rule-preview').disabled,true);
 assert.match(f.el('exit-rule-result').textContent,/已变化.*重新载入/);
 assert.equal(f.el('exit-rule-take').value,'1');
});

test('basket preview identifies every current member and unknown fills disable changes',async()=>{
 const snapshot=editableSnapshot('basket');snapshot.state.groups.push({...snapshot.state.groups[0],id:'g2'});
 const f=pageFixture(false,{'/api/trading/reconcile':()=>snapshot});
 await f.el('trading-reconcile').onclick();
 assert.match(f.el('exit-rule-group').innerHTML,/整篮子.*全部 2 组/);
 assert.doesNotMatch(f.el('exit-rule-group').innerHTML,/group:g1/);
 f.el('exit-rule-group').value='basket';f.el('exit-rule-load').onclick();f.el('exit-rule-preview').onclick();
 assert.match(f.el('exit-rule-result').textContent,/#g1 原规则[\s\S]*#g2 原规则/);
 const unknown=pageFixture(false,{'/api/trading/reconcile':()=>({...snapshot,state:{...snapshot.state,
   orders:[{id:'unknown',group:'g1',leg:'mt5',result:{status:'unknown'}}]}})});
 await unknown.el('trading-reconcile').onclick();
 unknown.el('exit-rule-group').value='basket';unknown.el('exit-rule-group').onchange();
 assert.equal(unknown.el('exit-rule-load').disabled,true);
});

test('dedicated pause remains usable during another in-flight action and never closes positions',async()=>{
 const snapshot=editableSnapshot();snapshot.state.enabled=true;let finish;
 const f=pageFixture(false,{'/api/trading/reconcile':()=>snapshot,
  '/api/trading/pause':()=>({...snapshot,state:{...snapshot.state,enabled:false}}),
  '/api/trading/apply-entry':()=>new Promise(resolve=>{finish=resolve;})});
 await f.el('trading-reconcile').onclick();
 const pending=f.el('apply-entry').onclick();
 assert.equal(f.el('trading-pause').disabled,false);
 await f.el('trading-pause').onclick();
 assert.match(f.el('new-entry-status').textContent,/已暂停/);
 assert.ok(!f.calls.some(c=>c.url.includes('/close')));
 finish({...snapshot,state:{...snapshot.state,enabled:false}});await pending;
 assert.equal(f.el('trading-pause').textContent,'暂停新开仓');
});

test('failed pause keeps the running state and does not claim success',async()=>{
 const snapshot=editableSnapshot();snapshot.state.enabled=true;
 const f=pageFixture(false,{'/api/trading/reconcile':()=>snapshot,'/api/trading/status':()=>snapshot,
  '/api/trading/pause':()=>{throw Error('连接已断开');}});
 await f.el('trading-reconcile').onclick();await f.el('trading-pause').onclick();
 assert.match(f.el('new-entry-status').textContent,/运行中/);
 assert.match(f.el('trading-result').textContent,/暂停尚未确认/);
});

test('fees label estimates, platform-verified costs and paper results accurately',async()=>{
 for(const [mode,verified,label] of [['live',false,'成交手续费（含估算）'],['live',true,'已核验成交手续费'],['paper',false,'模拟成交手续费']]){
  const snapshot=editableSnapshot();snapshot.state.groups[0].mode=mode;
  snapshot.state.groups[0].valuation.costs_verified=verified;
  if(verified)snapshot.state.groups[0].valuation.fees=-.3;
  const f=pageFixture(false,{'/api/trading/reconcile':()=>snapshot});
  await f.el('trading-reconcile').onclick();
  assert.ok(f.el('groups').innerHTML.includes(label));
  if(verified)assert.match(f.el('groups').innerHTML,/已核验成交手续费 \+0.30 USD/);
  assert.doesNotMatch(f.el('groups').innerHTML,/已由成交回报或平台记录确认/);
 }
});

test('background external-close checks are distinguished from complete reconciliation and expire',async()=>{
 const snapshot=monitoredSnapshot();
 snapshot.state.groups=[{id:'external',status:'attention',external_attention:true,parameters:{},valuation:{remaining:{binance:1,mt5:0}}}];
 snapshot.state.recovery=true;snapshot.capabilities.reconciled=false;
 snapshot.external_close_status={time_ms:Date.now(),level:'warning',message:'已回补可确认成交；仍有剩余仓位，请核对后处理'};
 const f=pageFixture(false,{'/api/trading/reconcile':()=>snapshot});
 await f.el('trading-reconcile').onclick();
 assert.match(f.el('position-check-status').textContent,/后台核对：已回补.*剩余仓位/);
 assert.match(f.el('records-position-check').textContent,/等待完整持仓对账/);
 assert.match(f.el('new-entry-status').textContent,/管理暂未恢复/);
 f.ageRisk(21000);
 assert.match(f.el('records-position-check').textContent,/后台核对：数据已过期/);
});

test('external fills show actual platform time and source without fabricated execution speed',async()=>{
 const snapshot=monitoredSnapshot();
 snapshot.state.groups=[{id:'external',status:'closed',external_close:true,mode:'live',contract:100,parameters:{},valuation:{remaining:{binance:0,mt5:0}}}];
 snapshot.state.orders=[{id:'manual',group:'external',leg:'mt5',action:'close',source:'external',symbol:'XAUUSD',created_ms:1000,requested:1,
  result:{status:'done',qty:1,price:4300,fee:.1,platform_time_ms:1000,reconcile_time_ms:9000}}];
 const f=pageFixture(false,{'/api/trading/reconcile':()=>snapshot});
 await f.el('trading-reconcile').onclick();
 const tables=f.el('orders').innerHTML;
 assert.match(tables,/平台外部平仓 · 已回补/);
 assert.match(tables,/平台成交时间 · 后台回补/);
 assert.doesNotMatch(tables,/查询确认 8000|适配器调用|意图→本机回报/);
});
