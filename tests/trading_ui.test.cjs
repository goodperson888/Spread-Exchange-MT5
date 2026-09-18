const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

// Execute the actual button handlers without contacting any trading account.
function pageFixture(reconcileFails = false, handlers = {}) {
  const elements = new Map(), calls = [], charts=[];
  const el = id => {
    if (!elements.has(id)) elements.set(id, {
      tagName:'BUTTON', value:'', textContent:'', innerHTML:'', dataset:{},
      classList:{toggle(){}, add(){}, remove(){}}, querySelectorAll(){return [];}
    });
    return elements.get(id);
  };
  let enabled = false, reconciled = false;
  const snapshot = () => ({connected:true, state:{enabled, groups:[], orders:[]},
    capabilities:{mode:'live', reconciled}});
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../trading-app.js'), 'utf8'), {
    document:{getElementById:el}, window:{addEventListener(){}},
    loaded:false, saveConfig:async()=>{}, AbortController,
    setTimeout(){}, clearTimeout(){}, setInterval(){},
    echarts:{init:()=>({getOption:()=>({}), setOption(option){charts.push(option);}})},
    fetch:async (url, options) => {
      calls.push({url, ...options});
      const action = url.split('/').at(-1);
      let body = {}, status = 200;
      if (handlers[url]) body=handlers[url](options.body===undefined?undefined:JSON.parse(options.body));
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
  return {el, calls, charts};
}

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
