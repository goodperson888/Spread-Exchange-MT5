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
  assert.equal(f.el('trading-toggle').disabled,false);
  assert.equal(f.el('trading-toggle').textContent,'暂停新开仓');
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
  assert.equal(f.el('trading-toggle').disabled,false);
  await f.el('trading-toggle').onclick();
  assert.ok(f.calls.some(x=>x.url==='/api/trading/pause'));
  assert.equal(f.run('window.goldPairUiBusy'),true);
  f.run('setUiBusy(-1)');assert.equal(f.run('window.goldPairUiBusy'),false);
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
