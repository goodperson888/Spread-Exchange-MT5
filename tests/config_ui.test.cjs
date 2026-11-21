const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const root=path.join(__dirname,'..');
const settle=()=>new Promise(resolve=>setImmediate(resolve));

async function fixture(handlers={}) {
  const elements=new Map(),calls=[],storage=new Map(),events={};
  const html=fs.readFileSync(path.join(root,'index.html'),'utf8');
  for(const match of html.matchAll(/<(\w+)\b([^>]*\bid="([^"]+)"[^>]*)>/g)) {
    const attrs=Object.fromEntries([...match[2].matchAll(/([\w-]+)="([^"]*)"/g)].map(x=>[x[1],x[2]]));
    const classes=new Set();
    elements.set(match[3],{id:match[3],tagName:match[1].toUpperCase(),value:attrs.value||'',
      type:attrs.type||'',min:attrs.min||'',max:attrs.max||'',readOnly:/\breadonly\b/.test(match[2]),
      checked:/\bchecked\b/.test(match[2]),disabled:false,textContent:'',innerHTML:'',dataset:{},
      hidden:/\bhidden\b/.test(match[2]),style:{},listeners:{},
      classList:{add:x=>classes.add(x),remove:x=>classes.delete(x),toggle(x,on){if(on)classes.add(x);else classes.delete(x);}},
      setAttribute(k,v){this[k]=v;},replaceChildren(){},querySelectorAll(){return [];},
      addEventListener(k,fn){(this.listeners[k]??=[]).push(fn);},focus(){this.focused=true;}});
    const element=elements.get(match[3]);let rawValue=String(element.value);
    for(const [key,value] of Object.entries(attrs))if(key.startsWith('data-'))element.dataset[key.slice(5)]=value;
    Object.defineProperty(element,'value',{get:()=>rawValue,set:v=>{rawValue=String(v);}});
  }
  const el=id=>{assert.ok(elements.has(id),'Unknown element '+id);return elements.get(id);};
  const document={hidden:false,getElementById:el,createElement:()=>({}),addEventListener(){},
    querySelectorAll(selector){
      if(selector==='[data-toggle-secret]')return [];
      if(selector==='[data-view]')return [...elements.values()].filter(e=>e.dataset.view);
      if(selector==='[data-open-view]')return [];
      return [...elements.values()].filter(e=>['INPUT','SELECT'].includes(e.tagName)&&
        (selector==='input, select'||!e.readOnly));
    }};
  const config=JSON.parse(fs.readFileSync(path.join(root,'config.example.json'),'utf8'));
  config.mt5={...config.mt5,adapter:'native',symbol:'XAUUSDm',contract_size_oz:100,volume_min:.01,volume_step:.01,volume_max:100};
  config.strategy.mt5_lots=.02;
  const window={addEventListener(k,fn){(events[k]??=[]).push(fn);},dispatchEvent(e){for(const fn of events[e.type]||[])fn();}};
  const context=vm.createContext({document,window,AbortController,Event:class{constructor(type){this.type=type;}},
    localStorage:{getItem:k=>storage.get(k)||null,setItem:(k,v)=>storage.set(k,v),removeItem:k=>storage.delete(k)},
    setTimeout(){return 1;},clearTimeout(){},setInterval(){},echarts:{init:()=>({resize(){},getOption:()=>({}),setOption(){}})},
    fetch:async(url,options)=>{
      const body=options.body===undefined?undefined:JSON.parse(options.body);calls.push({url,body});
      const result=handlers[url]?await handlers[url](body):url==='/api/config'?{config}:url==='/api/status'?{state:{running:false}}:{samples:[]};
      return {ok:true,json:async()=>({ok:true,...result})};
    }});
  vm.runInContext(fs.readFileSync(path.join(root,'app.js'),'utf8'),context);
  await settle();
  function edit(id,value){el(id).value=value;for(const fn of el(id).listeners.input||[])fn();}
  return {el,edit,context,calls,storage,config,
    run:code=>vm.runInContext(code,context),
    loadTrading(){vm.runInContext(fs.readFileSync(path.join(root,'trading-app.js'),'utf8'),context);}};
}

test('lots preview updates immediately after refresh without a fresh MT5 check',async()=>{
  const f=await fixture();
  assert.equal(f.run('hasResult'),false);
  f.edit('lots','0.01');
  assert.match(f.el('plan-result').textContent,/0\.01 手 ↔ XAUUSDT：1 XAU/);
  f.edit('lots','0.015');assert.match(f.el('plan-result').textContent,/不符合 MT5 规格/);
  f.edit('lots','');assert.match(f.el('plan-result').textContent,/必须大于 0/);
});

test('pool chart selection is display-only and never dirties trading configuration',async()=>{
  const f=await fixture();const revision=f.run('revision'),message=f.el('config-status').textContent;
  f.edit('pool-chart-pair','t2~t3');
  assert.equal(f.run('revision'),revision);
  assert.equal(f.run('dirtyFields.size'),0);
  assert.equal(f.el('config-status').textContent,message);
});

test('rebate percentages restore and save independently of the exchange fee rate',async()=>{
 const f=await fixture();
 assert.equal(f.el('binance-rebate').value,'0');assert.equal(f.el('mt5-rebate').value,'0');
 f.edit('binance-rebate','30');f.edit('mt5-rebate','50');
 const payload=f.run('formConfig()');
 assert.equal(payload.costs.binance_rebate_percent,30);assert.equal(payload.costs.mt5_rebate_percent,50);
 assert.equal(payload.costs.binance_taker_percent,f.config.costs.binance_taker_percent);
 f.run('populate({...formConfig(),costs:{...formConfig().costs,binance_rebate_percent:12.5,mt5_rebate_percent:0}})');
 assert.equal(f.el('binance-rebate').value,'12.5');assert.equal(f.el('mt5-rebate').value,'0');
});

test('combined entry spread guard is opt-in and saves separately from per-leg slippage',async()=>{
 const f=await fixture();assert.equal(f.el('entry-spread-guard-on').checked,false);
 f.el('entry-spread-guard-on').checked=true;f.edit('entry-spread-slippage','0.25');
 const saved=f.run('formConfig()');
 assert.equal(saved.strategy.entry_spread_guard_enabled,true);assert.equal(saved.strategy.entry_spread_slippage_usd,.25);
 assert.equal(saved.strategy.max_slippage_usd,f.config.strategy.max_slippage_usd);
 f.run('populate({...formConfig(),strategy:{...formConfig().strategy,entry_spread_guard_enabled:false}})');
 assert.equal(f.el('entry-spread-guard-on').checked,false);
});

test('Lighter depth usage and cost buffer restore and save as independent pair strategy fields',async()=>{
 const f=await fixture();
 assert.equal(f.el('liquidity-guard-on').checked,true);
 assert.equal(f.el('liquidity-usage').value,'80');assert.equal(f.el('liquidity-buffer').value,'0.1');
 f.edit('liquidity-usage','60');f.edit('liquidity-buffer','0.25');
 assert.equal(f.run('dirtyFields.has("liquidity-usage")'),true);
 const c=f.run('formConfig()');
 assert.equal(c.strategy.liquidity_max_usage_percent,60);assert.equal(c.strategy.liquidity_buffer_usd,.25);
 f.run('populate({...formConfig(),strategy:{...formConfig().strategy,liquidity_guard_enabled:false,liquidity_max_usage_percent:90,liquidity_buffer_usd:0}})');
 assert.equal(f.el('liquidity-guard-on').checked,false);assert.equal(f.el('liquidity-buffer').value,'0');
 assert.equal(f.el('liquidity-usage').value,'90');
});

test('opening dispatch and MT5 quote interval restore, dirty and save as pair execution settings',async()=>{
 const f=await fixture();
 assert.equal(f.el('entry-dispatch').value,'auto');assert.equal(f.el('mt5-quote-poll-ms').value,'50');
 f.edit('entry-dispatch','serial');f.edit('mt5-quote-poll-ms','20');
 assert.equal(f.run('dirtyFields.has("entry-dispatch")'),true);
 assert.equal(f.run('dirtyFields.has("mt5-quote-poll-ms")'),true);
 const c=f.run('formConfig()');assert.equal(c.execution.entry_dispatch,'serial');assert.equal(c.execution.mt5_quote_poll_ms,20);
 f.run('populate({...formConfig(),execution:{...formConfig().execution,entry_dispatch:"auto",mt5_quote_poll_ms:50}})');
 assert.equal(f.el('entry-dispatch').value,'auto');assert.equal(f.el('mt5-quote-poll-ms').value,'50');
});

test('MT5 commission references show source samples and preserve manual fallback and separate switches',async()=>{
 const ref={symbol:'GOLD-A',checked_ms:Date.now(),profiles:{
   open_0:{state:'ready',rate:6,samples:3},close_1:{state:'ready',rate:0,samples:4},
   open_1:{state:'zero_unconfirmed',samples:3},close_0:{state:'insufficient',samples:0}}};
 const live={connected:true,state:{enabled:false,groups:[],orders:[]},capabilities:{mode:'live',reconciled:true},
   auto_values:{mt5_commission_reference:{mt5:ref}}};
 const f=await fixture({'/api/trading/reconcile':()=>live,'/api/trading/status':()=>live});
 f.edit('mt5-fee','7');f.el('auto-mt5-fee').checked=true;f.el('auto-mt5-short-fee').checked=false;
 f.loadTrading();await f.el('trading-reconcile').onclick();
 assert.match(f.el('mt5-commission-summary').textContent,/2\/4 类样本可用/);
 assert.match(f.el('mt5-commission-reference').textContent,/买入平仓|卖出平仓/);
 assert.match(f.el('mt5-commission-reference').textContent,/全零记录未确认免佣/);
 assert.equal(f.el('mt5-fee').value,'7');
 const payload=f.run('formConfig()');
 assert.equal(payload.costs.mt5_fee_auto,true);assert.equal(payload.costs.mt5_short_fee_auto,false);
 assert.equal(payload.costs.mt5_commission_per_lot_side,7);
});

test('failed MT5 check shows raw symbol fields and never applies unverified contract',async()=>{
  const result={connected:true,identity_matches:true,symbol:null,message:'已连接 MT5，但品种未通过校验',
    symbol_diagnostics:{name:'XAUUSD',description:'<img src=x>',currency_base:'',currency_profit:'USD',
      currency_margin:'USD',custom:false,trade_contract_size:100,trade_mode:4,trade_calc_mode:3,
      volume_min:.01,volume_step:.01,volume_max:100,reasons:['基础货币字段 currency_base 未返回，无法确认 XAU']}};
  const f=await fixture({'/api/mt5/check':()=>({result})});
  await f.el('check-mt5').onclick();
  const text=f.el('mt5-result').textContent;
  assert.match(text,/currency_base：空值/);
  assert.match(text,/currency_profit："USD"/);
  assert.match(text,/currency_margin："USD"/);
  assert.match(text,/custom：false/);
  assert.match(text,/trade_contract_size：100（尚未确认计量单位）/);
  assert.match(text,/未通过：基础货币/);
  assert.match(text,/<img src=x>/);assert.equal(f.el('mt5-result').innerHTML,'');
  assert.equal(f.run('hasResult'),false);
  assert.ok(!f.calls.some(c=>c.url==='/api/mt5/apply'));
  f.run('showResult({symbol:null,message:"未知",symbol_diagnostics:{custom:null,reasons:[]}})');
  assert.match(f.el('mt5-result').textContent,/custom：未返回/);
});

function nativeCheckResult() {
  const row=(name,contract,bid,ask)=>({name,passed:true,errors:[],
    spec:{name,contract_size_oz:contract,volume_min:.01,volume_step:.01,volume_max:10,currency_profit:'USD'},
    quote:{bid,ask,age_ms:-7200000,valid_price:true},verification:{passed:true}});
  const checks={binance:row('GOLD-FUT',100,4301,4301.1),mt5:row('GOLD-SPOT',10,4300,4300.2)};
  return {connected:true,identity_matches:true,pair_check:true,pair_ready:true,message:'双品种校验通过',
    identity:{account:'123',server:'Demo',currency:'USD',margin_mode:2},pair_lots:.1,
    pair_checks:checks,symbol:checks.mt5.spec,pair_lot_rules:{min:.1,max:10,step:.1},blockers:[],warnings:[]};
}
function nativeUi(f) {
  f.context.window.GoldPairPairs={info:()=>({type:'mt5_mt5',short_symbol:'GOLD-FUT'}),native:()=>true};
}
test('native check saves latest lots, renders both legs, and applies common step',async()=>{
  const f=await fixture({'/api/mt5/check':()=>({result:nativeCheckResult()})});nativeUi(f);
  f.edit('lots','.1');
  await f.el('check-mt5').onclick();
  const saved=f.calls.filter(c=>c.url==='/api/config'&&c.body).at(-1);
  assert.equal(saved.body.strategy.mt5_lots,.1);
  const text=f.el('mt5-result').textContent;
  assert.match(text,/品种 A GOLD-FUT：通过/);assert.match(text,/品种 B GOLD-SPOT：通过/);
  assert.match(text,/共同手数规则：最小 0.1 \/ 步长 0.1/);assert.doesNotMatch(text,/黄金品种|盎司/);
  assert.match(text,/不代表持续行情已就绪/);
  assert.equal(f.el('bid').value,'4301');assert.equal(f.el('mt5-bid').value,'4300');
  assert.equal(f.el('lots').step,.1);assert.equal(f.el('lots').min,.1);
  assert.ok(f.calls.some(c=>c.url==='/api/mt5/apply'));assert.equal(f.run('hasResult'),true);
  f.edit('lots','.2');assert.equal(f.run('hasResult'),false);
});
test('native one-leg failure cannot auto-apply a usable long-leg symbol',async()=>{
  const result=nativeCheckResult();result.pair_ready=false;
  result.pair_checks.binance.passed=false;result.pair_checks.binance.errors=['不支持当前手数'];
  const f=await fixture({'/api/mt5/check':()=>({result})});nativeUi(f);
  await f.el('check-mt5').onclick();
  assert.match(f.el('mt5-result').textContent,/品种 A GOLD-FUT：未通过/);
  assert.match(f.el('mt5-result').textContent,/品种 B GOLD-SPOT：通过/);
  assert.equal(f.run('hasResult'),false);assert.ok(!f.calls.some(c=>c.url==='/api/mt5/apply'));
});

test('empty symbol auto detection renders candidate diagnosis without applying hints',async()=>{
  const result={connected:true,identity_matches:true,symbol:null,code:'SELECT_SYMBOL',message:'自动识别未找到通过校验的黄金品种',
    symbol_scan:{total_symbols:123,validated_candidates:0,diagnostic_matches:1,shown:1},
    symbol_diagnostic_candidates:[{name:'XAUUSD',description:'Gold vs US Dollar / Spot',currency_base:'',
      currency_profit:'USD',custom:false,trade_contract_size:100,reasons:['基础货币字段 currency_base 为空值，无法确认 XAU']}]};
  const f=await fixture({'/api/mt5/check':()=>({result})});
  const list=f.el('mt5-symbols');list.replaceChildren=(...options)=>{list.options=options;};
  f.edit('mt5-symbol','');await f.el('check-mt5').onclick();
  const text=f.el('mt5-result').textContent;
  assert.match(text,/已扫描 123 个品种/);
  assert.match(text,/候选仅供诊断/);
  assert.match(text,/MT5 实际返回.*XAUUSD/);
  assert.match(text,/currency_base：空值/);
  assert.match(text,/未通过：基础货币/);
  assert.equal(f.run('hasResult'),false);
  assert.ok(!f.calls.some(c=>c.url==='/api/mt5/apply'));
  assert.equal(list.options[0].value,'XAUUSD');
  assert.match(list.options[0].label,/待校验，仅供诊断/);
});

test('verified USD-base CFD shows compatibility confirmation and applies its real contract',async()=>{
  const result={connected:true,identity_matches:true,message:'已连接 MT5',
    identity:{account:'123',server:'Demo',currency:'USD'},candidates:['XAUUSD'],
    symbol:{name:'XAUUSD',contract_size_oz:100,volume_min:.01,volume_step:.01,volume_max:80,currency_base:'USD'},
    symbol_diagnostics:{currency_base:'USD'},symbol_verification:{method:'gold_cfd_profit',passed:true},
    symbol_resolution:{requested:'xauusd',resolved:'XAUUSD'}};
  const f=await fixture({'/api/mt5/check':()=>({result}),
    '/api/paper/plan':()=>({plan:{mt5_lots:.01,binance_qty_xau:1,gold_qty_oz:1}})});
  f.edit('lots','0.01');f.edit('mt5-symbol','xauusd');await f.el('check-mt5').onclick();
  const text=f.el('mt5-result').textContent;
  assert.match(text,/黄金 CFD 兼容校验通过/);
  assert.match(text,/基础货币实际返回 "USD"/);
  assert.match(text,/多空、涨跌四项盈亏预估/);
  assert.match(text,/未发送订单，仅在连接检查时执行/);
  assert.match(text,/xauusd → XAUUSD/);
  assert.equal(f.el('mt5-symbol').value,'XAUUSD');
  assert.doesNotMatch(text,/未通过|尚未确认计量单位/);
  assert.equal(f.run('hasResult'),true);
  assert.equal(f.calls.filter(c=>c.url==='/api/mt5/apply').length,1);
  assert.equal(f.el('contract').value,'100');
  assert.match(f.el('plan-result').textContent,/0\.01 手 ↔ XAUUSDT：1 XAU/);
  assert.ok(!f.calls.some(c=>['/api/trading/start','/api/trading/connect','/api/trading/close'].includes(c.url)));
});

test('quote failure shows actual prices and time separately from raw symbol metadata',async()=>{
  const f=await fixture();
  f.run(`showResult({connected:true,identity_matches:true,symbol:null,message:'黄金 CFD 兼容校验未通过',
    quote_diagnostics:{bid:0,ask:0,time_ms:123000,age_ms:60000}})`);
  assert.match(f.el('mt5-result').textContent,/报价诊断：Bid 0 \/ Ask 0/);
  assert.match(f.el('mt5-result').textContent,/MT5 原始时间戳 123000/);
  assert.match(f.el('mt5-result').textContent,/与本机时间差 60000 毫秒/);
  assert.equal(f.run('hasResult'),false);
  f.run(`showResult({connected:true,identity_matches:true,
    symbol:{name:'XAUUSD',contract_size_oz:100},message:'规格核验通过',
    symbol_verification:{passed:true},symbol_diagnostics:{currency_base:'USD'},
    quote:{bid:4300,ask:4300.2,age_ms:60000},
    quote_diagnostics:{bid:4300,ask:4300.2,time_ms:123000,age_ms:60000},
    blockers:['报价未更新，不能视为可立即成交']})`);
  const text=f.el('mt5-result').textContent;
  assert.match(text,/兼容校验通过/);
  assert.match(text,/报价诊断：Bid 4300 \/ Ask 4300.2/);
  assert.match(text,/待处理：报价未更新/);
  const previousBid=f.el('mt5-bid').value,previousAsk=f.el('mt5-ask').value;
  f.run(`showResult({connected:true,identity_matches:true,
    symbol:{name:'XAUUSD',contract_size_oz:100},message:'规格核验通过',
    symbol_verification:{passed:true,price_source:'hypothetical_prices'},
    symbol_diagnostics:{currency_base:'USD'},quote:null,
    quote_diagnostics:{bid:0,ask:0,time_ms:0,age_ms:null},blockers:['尚无有效 Bid/Ask']})`);
  assert.match(f.el('mt5-result').textContent,/换算核验使用假设价格/);
  assert.match(f.el('mt5-result').textContent,/不代表已获取可交易行情/);
  assert.equal(f.el('mt5-bid').value,previousBid);
  assert.equal(f.el('mt5-ask').value,previousAsk);
});

test('risk thresholds save numerically and never ask to reconnect the trading engine',async()=>{
  const live={connected:true,config_pending:false,state:{enabled:false,groups:[],orders:[]},capabilities:{mode:'live',reconciled:true}};
  const f=await fixture({'/api/trading/reconcile':()=>live,'/api/trading/status':()=>live});
  f.loadTrading();await f.el('trading-reconcile').onclick();
  f.edit('risk-binance-warning','8');f.edit('risk-mt5-warning','300');
  assert.doesNotMatch(f.el('workflow-status').textContent,/重新连接/);
  assert.equal(f.el('trading-toggle').disabled,false);
  await f.run('saveConfig()');
  const payload=f.calls.filter(x=>x.url==='/api/config'&&x.body).at(-1).body;
  assert.equal(payload.risk_alerts.binance_warning_distance_percent,8);
  assert.equal(payload.risk_alerts.mt5_warning_margin_level_percent,300);
  assert.equal(f.run('dirtyFields.size'),0);
  assert.ok(!f.calls.some(x=>['/api/trading/start','/api/trading/connect','/api/trading/close'].includes(x.url)));
});

test('server clock difference is labelled as unconfirmed time basis rather than negative quote age',async()=>{
  const f=await fixture();
  f.run(`showResult({connected:true,identity_matches:true,message:'黄金规格已确认',
    symbol:{name:'XAUUSD',contract_size_oz:100},
    quote:{bid:4127.13,ask:4127.23,age_ms:-7191593,time_ms:1793887333856},
    quote_diagnostics:{bid:4127.13,ask:4127.23,time_ms:1793887333856,age_ms:-7191593,
      raw_time_delta_ms:-7191593,possible_offset_hours:2},
    warnings:['单次读取无法区分时间基准差异与报价未更新；仍需通过 tick 持续更新检查。']})`);
  const text=f.el('mt5-result').textContent;
  assert.match(text,/比本机快 1 小时 59 分 51\.593 秒/);
  assert.match(text,/接近 2 小时，原因待确认/);
  assert.match(text,/不是已测得的网络延迟/);
  assert.match(text,/时间提示：.*持续更新检查/);
  assert.doesNotMatch(text,/报价年龄|自动校正/);
  assert.equal(f.el('mt5-bid').value,'4127.13');
  assert.equal(f.el('mt5-ask').value,'4127.23');
  assert.ok(!f.calls.some(c=>['/api/mt5/check','/api/trading/start','/api/trading/connect'].includes(c.url)));
});

test('old runtime plan never replaces the current quantity preview',async()=>{
  const f=await fixture({'/api/trading/reconcile':()=>({connected:true,plan:{lots:.02,qty:2},
    state:{groups:[],orders:[]},config_pending:true,capabilities:{mode:'paper',reconciled:true}})});
  f.loadTrading();f.edit('lots','0.01');
  for(let i=0;i<3;i++)await f.el('trading-reconcile').onclick();
  assert.match(f.el('plan-result').textContent,/0\.01 手 ↔ XAUUSDT：1 XAU/);
  assert.match(f.el('runtime-plan').textContent,/0\.02 手 ↔ 币安 2 XAU/);
  assert.equal(f.el('trading-toggle').disabled,true);
  assert.match(f.el('workflow-status').textContent,/参数有变更/);
});

test('an older async plan response cannot replace a newer edit',async()=>{
  let resolve;
  const f=await fixture({'/api/paper/plan':()=>new Promise(r=>{resolve=r;})});
  f.run('hasResult=true');const pending=f.run('calculatePlan(false)');await settle();
  f.edit('lots','0.01');resolve({plan:{mt5_lots:.02,binance_qty_xau:2,gold_qty_oz:2,note:'old'}});
  await pending;
  assert.match(f.el('plan-result').textContent,/0\.01 手 ↔ XAUUSDT：1 XAU/);
});

test('a queued old save cannot clear the newer edit or its browser draft',async()=>{
  let release;let posts=0;
  const f=await fixture({'/api/config':body=>body?(posts++,new Promise(r=>{release=r;})):{config:JSON.parse(fs.readFileSync(path.join(root,'config.example.json'),'utf8'))}});
  const pending=f.run('saveConfig()');
  f.edit('lots','0.01');await settle();assert.equal(posts,1);
  release({});await pending;
  assert.equal(f.run("dirtyFields.has('lots')"),true);
  const draft=JSON.parse(f.storage.get('GoldPairLocal.configDraft.v2'));
  assert.equal(draft.fields.lots,'0.01');
});

test('pending configuration never disables an already-running pause action',async()=>{
  const f=await fixture({'/api/trading/reconcile':()=>({connected:true,plan:{lots:.02,qty:2},
    state:{enabled:true,groups:[],orders:[]},config_pending:true,capabilities:{reconciled:true}})});
  f.loadTrading();await f.el('trading-reconcile').onclick();
  assert.equal(f.el('trading-toggle').disabled,true);
  assert.equal(f.el('trading-pause').disabled,false);
  assert.equal(f.el('trading-pause').textContent,'暂停新开仓');
});

test('workspace tabs support keyboard navigation without sending trading requests',async()=>{
  const f=await fixture();
  vm.runInContext(fs.readFileSync(path.join(root,'workspace.js'),'utf8'),f.context);
  const before=f.calls.length;
  for(const fn of f.el('tab-trading').listeners.keydown)fn({key:'ArrowRight',preventDefault(){}});
  assert.equal(f.el('view-settings').hidden,false);assert.equal(f.el('view-trading').hidden,true);
  assert.equal(f.el('tab-settings')['aria-selected'],'true');assert.equal(f.el('tab-settings').focused,true);
  for(const fn of f.el('tab-records').listeners.keydown)fn({key:'Home',preventDefault(){}});
  assert.equal(f.el('view-trading').hidden,false);
  assert.equal(f.calls.length,before);
});

test('pause remains available during a separate read-only connection check',async()=>{
  const snapshot=enabled=>({connected:true,state:{enabled,groups:[],orders:[]},capabilities:{reconciled:true}});
  const f=await fixture({'/api/trading/reconcile':()=>snapshot(true),'/api/trading/pause':()=>snapshot(false)});
  f.loadTrading();await f.el('trading-reconcile').onclick();f.run('setUiBusy(1)');
  assert.equal(f.el('trading-pause').disabled,false);
  await f.el('trading-pause').onclick();
  assert.ok(f.calls.some(x=>x.url==='/api/trading/pause'));
  assert.equal(f.run('window.goldPairUiBusy'),true);
  f.run('setUiBusy(-1)');assert.equal(f.run('window.goldPairUiBusy'),false);
});

test('editing existing exit rules does not autosave main strategy or mark it pending',async()=>{
 const f=await fixture();const before=f.run('revision');
 f.edit('exit-rule-take','1.2');f.edit('exit-rule-loss','15');
 assert.equal(f.run('revision'),before);
 assert.equal(f.run('dirtyFields.size'),0);
 assert.equal(f.storage.has('GoldPairLocal.configDraft.v2'),false);
});

test('editing while save-and-connect is in flight prevents connecting with old input',async()=>{
  let release;
  const config=JSON.parse(fs.readFileSync(path.join(root,'config.example.json'),'utf8'));
  const f=await fixture({'/api/config':body=>body?new Promise(r=>{release=r;}):{config}});
  f.loadTrading();const action=f.el('trading-connect').onclick();await settle();
  f.edit('lots','0.01');release({});await action;
  assert.equal(f.calls.some(x=>x.url==='/api/trading/connect'),false);
  assert.match(f.el('trading-result').textContent,/保存期间参数又有修改/);
});

test('chart reference checkbox never dirties, drafts or saves trading configuration',async()=>{
 const f=await fixture();
 const before=f.run('revision'),configBefore=JSON.stringify(f.run('formConfig()'));
 f.el('chart-reference-enabled').checked=false;
 for(const changed of f.el('chart-reference-enabled').listeners.change||[])changed();
 assert.equal(f.run('revision'),before);
 assert.equal(f.run('dirtyFields.size'),0);
 assert.equal(JSON.stringify(f.run('formConfig()')),configBefore);
 assert.equal(f.calls.filter(c=>c.url==='/api/config'&&c.body!==undefined).length,0);
 assert.equal(f.run('editableField("chart-reference-enabled")'),false);
});


test('configuration drafts stay bound to the pair and stale pair drafts are ignored',async()=>{
 const f=await fixture();
 f.run('populate({...formConfig(),pair:{id:"pair-a",name:"组合甲",type:"exchange_mt5"}})');
 f.edit('lots','0.01');f.run('saveDraft()');
 const draft=JSON.parse(f.storage.get('GoldPairLocal.configDraft.v2'));
 assert.equal(draft.pair_id,'pair-a');assert.equal(draft.fields.lots,'0.01');
 assert.equal(f.run('readDraft("pair-b")'),null);
 assert.equal(f.run('readDraft("pair-a").fields.lots'),'0.01');
 f.run('dirtyFields.clear();populate({...formConfig(),pair:{id:"pair-b",name:"组合乙",type:"exchange_mt5"},strategy:{...formConfig().strategy,mt5_lots:.03}})');
 assert.equal(f.el('lots').value,'0.03');
 assert.match(f.el('pair-parameter-scope').textContent,/组合乙/);
 f.edit('take','4');f.run('saveDraft()');
 const next=JSON.parse(f.storage.get('GoldPairLocal.configDraft.v2'));
 assert.equal(next.pair_id,'pair-b');assert.equal(next.fields.lots,undefined);
 assert.equal(next.fields.take,'4');
});

test('configuration saves carry the pair identity and independent directional fields',async()=>{
 const f=await fixture();
 f.run('populate({...formConfig(),pair:{id:"pair-b",name:"组合乙",type:"mt5_mt5"}})');
 f.edit('entry','12');f.edit('entry-lower','3');f.edit('direction-mode','auto');
 await f.run('saveConfig()');
 const body=f.calls.filter(c=>c.url==='/api/config'&&c.body).at(-1).body;
 assert.equal(body.expected_pair_id,'pair-b');assert.equal(body.strategy.direction_mode,'auto');
 assert.equal(body.strategy.entry_lower_usd,3);assert.equal(body.strategy.entry_spread_usd,12);
 assert.match(f.el('config-status').textContent,/组合乙/);
});

test('shared entry confirmation saves both combinations and independent profit confirmation',async()=>{
 const f=await fixture();
 assert.equal(f.el('entry-confirm-mode').value,'at_or_better');
 assert.equal(f.el('entry-confirm-ticks').value,'1');
 f.edit('entry-confirm-mode','equal');f.edit('entry-confirm-ticks','5');
 f.edit('exit-confirm-ticks','3');f.edit('entry-confirm-decimals','2');
 const s=f.run('formConfig().strategy');
 for(const d of ['forward','reverse']){
  assert.equal(s[`entry_${d}_confirmation_mode`],'equal');
  assert.equal(s[`entry_${d}_confirmation_ticks`],5);
 }
 assert.equal(s.exit_confirmation_ticks,3);assert.equal(s.entry_confirmation_decimals,2);
 assert.equal(f.run("dirtyFields.has('entry-confirm-mode')"),true);
 assert.equal(f.run("dirtyFields.has('exit-confirm-ticks')"),true);
 await f.run('saveConfig()');
 const body=f.calls.filter(c=>c.url==='/api/config'&&c.body).at(-1).body;
 assert.equal(body.strategy.entry_reverse_confirmation_ticks,5);
 assert.equal(body.strategy.exit_confirmation_ticks,3);
});

test('legacy differing confirmations display selected combination and unify only on explicit save',async()=>{
 const f=await fixture();
 f.run('populate({...formConfig(),strategy:{...formConfig().strategy,direction_mode:"reverse",entry_forward_confirmation_ticks:2,entry_reverse_confirmation_ticks:7,entry_reverse_confirmation_mode:"equal"}})');
 assert.equal(f.el('entry-confirm-ticks').value,'7');assert.equal(f.el('entry-confirm-mode').value,'equal');
 assert.equal(f.el('entry-confirm-legacy').hidden,false);
 assert.match(f.el('entry-confirm-legacy').textContent,/保存后统一/);
 assert.equal(f.calls.filter(c=>c.url==='/api/config'&&c.body).length,0);
 const s=f.run('formConfig().strategy');assert.equal(s.entry_forward_confirmation_ticks,7);
 f.run('populate({...formConfig(),strategy:{...formConfig().strategy,direction_mode:"auto",entry_forward_confirmation_ticks:4,entry_reverse_confirmation_ticks:9}})');
 assert.equal(f.el('entry-confirm-ticks').value,'4');
});

test('Lighter API private key stays out of drafts and blank fields preserve server-owned secret',async()=>{
  const f=await fixture();
  f.edit('lighter-account-index','123');f.edit('lighter-api-key-index','4');
  f.edit('lighter-private-key','a'.repeat(80));
  const payload=f.run('formConfig()');
  assert.equal(payload.lighter.account_index,123);assert.equal(payload.lighter.api_key_index,4);
  assert.equal(payload.lighter.api_private_key,'a'.repeat(80));
  f.run('saveDraft()');
  for(const raw of f.storage.values())assert.ok(!raw.includes('a'.repeat(80)));
  assert.equal(f.run("dirtySecrets.has('lighter-private-key')"),true);
  f.run("populate({...formConfig(),lighter:{account_index:123,api_key_index:4,api_private_key_configured:true}})");
  assert.equal(f.el('lighter-private-key').value,'');assert.match(f.el('lighter-key-status').textContent,/已保存在本机/);
  assert.equal(f.run('formConfig().lighter.api_private_key'),'');
});
