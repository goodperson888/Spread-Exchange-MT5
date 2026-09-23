const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

// Execute the actual button handlers without contacting any trading account.
function pageFixture(reconcileFails = false, handlers = {}) {
  const elements = new Map(), calls = [], charts=[], streams=[], listeners={};
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
      classList:{toggle(){}, add(){}, remove(){}}, querySelectorAll(){return [];}, addEventListener(name,fn){this['on'+name]=fn;}
    });
    return elements.get(id);
  };
  let enabled = false, reconciled = false;
  const snapshot = () => ({connected:true, state:{enabled, groups:[], orders:[]},
    capabilities:{mode:'live', reconciled}});
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../trading-app.js'), 'utf8'), {
    document:doc, window:{addEventListener(){},EventSource:FakeStream,GoldPairQuotes:require('../quote-stream.js')},
    loaded:false, saveConfig:async()=>{}, AbortController,
    setTimeout(){}, clearTimeout(){}, setInterval(){},
    echarts:{init:()=>({getOption:()=>({}), setOption(option){charts.push(option);}})},
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
  return {el, calls, charts, streams, doc, listeners};
}

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
  assert.equal(el('trading-toggle').textContent, '暂停新开仓');
  await el('trading-toggle').onclick();
  assert.equal(el('trading-toggle').textContent, '启动自动开仓');
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
