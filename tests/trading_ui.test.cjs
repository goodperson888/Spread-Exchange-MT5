const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

// Execute the actual button handlers without contacting any trading account.
function pageFixture(reconcileFails = false) {
  const elements = new Map(), calls = [];
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
    echarts:{init:()=>({getOption:()=>({}), setOption(){}})},
    fetch:async (url, options) => {
      calls.push({url, ...options});
      const action = url.split('/').at(-1);
      let body = {}, status = 200;
      if (['reconcile','start','pause'].includes(action)) {
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
  return {el, calls};
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
