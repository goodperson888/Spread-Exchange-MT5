const {test}=require('node:test');
const assert=require('node:assert/strict');
const {QuoteBuffer,ChartBuffer,renderPoints,chartSeries,sameChartMarket,markGroups}=require('../quote-stream.js');
const q=(t,entry=1,key='a')=>({time_ms:t,entry,exit:entry+.2,key});
test('duplicate timestamps never rewrite history; resync retains received live points',()=>{
 const b=new QuoteBuffer();b.merge([q(100),q(102,2)]);
 const rows=b.replaceHistory([q(100,99),q(101)],0,'a');
 assert.deepEqual(rows.map(x=>x.time_ms),[100,101,102]);assert.equal(rows[0].entry,1);
});
test('buffer bounds and account switch discard unrelated samples',()=>{
 const b=new QuoteBuffer(2);assert.equal(b.merge([q(1),q(2),q(3)]).length,2);
 const rows=b.merge([q(4,2,'b')],0,'b');assert.deepEqual(rows.map(x=>x.key),['b']);
});
test('live capacity never evicts selected archived history and total memory stays bounded',()=>{
 const b=new ChartBuffer(3,4);
 b.merge([q(100,7)],0,'a');
 b.replaceHistory([q(1),q(2),q(3),q(4),q(100,99)],0,'a');
 for(let t=101;t<120;t++)b.merge([q(t)],0,'a');
 assert.deepEqual(b.rows.map(x=>x.time_ms),[2,3,4,100,117,118,119]);
 assert.ok(b.rows.length<=7);
 assert.equal(b.merge([],118,'a').length,2);
 assert.deepEqual(b.merge([q(121,1,'b')],0,'b').map(x=>x.time_ms),[121]);
});
test('chart history merges without duplicates, keeps live values and expands again after narrowing',()=>{
 const b=new ChartBuffer(3,4);
 b.merge([q(100,7)],0,'a');
 assert.equal(b.replaceHistory([q(1),q(100,99)],0,'a').at(-1).entry,7);
 assert.deepEqual(b.merge([],50,'a').map(x=>x.time_ms),[100]);
 assert.deepEqual(b.replaceHistory([q(1),q(2),q(100,99)],0,'a').map(x=>x.time_ms),[1,2,100]);
 assert.equal(b.rows.at(-1).entry,7);
});
test('every new live tick is rendered; closed UTC buckets remain stable',()=>{
 const rows=Array.from({length:7000},(_,i)=>q(i*1000));
 const first=renderPoints(rows,86400000);const next=renderPoints([...rows,q(6999001,2)],86400000);
 assert.equal(next.at(-1).time_ms,6999001);
 assert.deepEqual(first.filter(x=>x.time_ms<6900000),next.filter(x=>x.time_ms<6900000));
 assert.ok(next.some(x=>x.time_ms===6999000));
});

test('zoomed viewport retains subsecond historical quotes',()=>{
 const rows=Array.from({length:7000},(_,i)=>q(i*50));
 const visible=renderPoints(rows,86400000,{start:100000,end:101000});
 assert.equal(visible.filter(x=>x.time_ms>=100000&&x.time_ms<=101000).length,21);
});

test('terminal relocation retains market history while identity changes stay isolated',()=>{
 const key='live|native|XAUUSDT|native|C:/old.exe|123|broker|XAUUSD|42';
 const moved=key.replace('C:/old.exe','D:/new.exe');
 assert.equal(sameChartMarket(key,moved),true);
 for(const index of [0,1,2,3,5,6,7,8]){
  const parts=key.split('|');parts[index]+='-different';
  assert.equal(sameChartMarket(key,parts.join('|')),false);
 }
 const b=new QuoteBuffer();b.merge([q(1,1,key)],0,key);
 b.replaceHistory([q(2,2,moved),q(3,3,key.replace('|123|','|456|'))],0,moved);
 assert.deepEqual(b.rows.map(x=>x.entry),[1,2]);
});

test('long sample gaps insert a null and never synthesize executable prices',()=>{
 const rows=[0,1000,2000,300000,301000].map(t=>q(t));
 const series=chartSeries(rows,86400000);
 assert.deepEqual(series.gaps,[{start:2000,end:300000,reason:'本机采样缺失'}]);
 assert.deepEqual(series.lines.filter(x=>x.entry===null),[{time_ms:2001,entry:null,exit:null}]);
 assert.deepEqual(series.samples,rows);
 assert.ok(series.lines.every((x,i,a)=>i===0||x.time_ms>=a[i-1].time_ms));
});

test('invalid leading middle and trailing quotes cannot appear as executable lines',()=>{
 for(const rows of [
  [{...q(0),valid:false},q(1000),q(2000)],
  [q(0),{...q(1000),valid:false},q(2000)],
  [q(0),q(1000),{...q(2000),valid:false}],
  [{...q(0),valid:false},{...q(1000),valid:false}]
 ]){
  const series=chartSeries(rows,86400000);
  assert.equal(series.gaps.length,1);
  assert.match(series.gaps[0].reason,/无效/);
  assert.deepEqual(series.samples,rows.filter(x=>x.valid!==false));
  assert.equal(series.lines.filter(x=>x.entry===null).length,1);
  assert.ok(series.lines.every((x,i,a)=>i===0||x.time_ms>=a[i-1].time_ms));
 }
});

test('detect gaps before thinning while retaining normal minute archive cadence',()=>{
 const rows=Array.from({length:7000},(_,i)=>q(i*1000+(i>=1000?600000:0)));
 const series=chartSeries(rows,30*86400000);
 assert.equal(series.gaps.length,1);
 assert.ok(series.samples.length<rows.length);
 assert.equal(series.lines.filter(x=>x.entry===null).length,1);
 assert.equal(chartSeries(Array.from({length:100},(_,i)=>q(i*60000)),30*86400000).gaps.length,0);
});

test('open group mark-to-market moves with the latest quote while closed group stays fixed',()=>{
 const basis={key:'a',time_ms:1000,usdt_usd:1,binance:{ask:100},mt5:{bid:99}};
 const latest={key:'a',time_ms:1250,usdt_usd:1,binance:{ask:101},mt5:{bid:100}};
 const open={id:'open',key:'a',status:'open',contract:100,qty:1,costs:{usdt_usd:1,binance_taker_percent:0,mt5_commission_per_lot_side:0},valuation:{gross:-1,fees:0,carry:0,estimated_exit_fee:1,net:-2,remaining:{binance:1,mt5:1}}};
 const closed={id:'closed',key:'a',status:'closed',valuation:{gross:7,net:6}};
 const rows=markGroups([open,closed],basis,latest);
 assert.equal(rows[0].valuation.gross,-1);
 assert.equal(rows[0].valuation.net,-1);
 assert.deepEqual(rows[1],closed);
 const unverified={...open,valuation_status:'unverified'};
 assert.deepEqual(markGroups([unverified],basis,latest)[0],unverified);
});

test('live projected profit preserves incurred rebates and reprices future fee rebates',()=>{
 const basis={key:'a',time_ms:1000,usdt_usd:1,binance:{ask:100},mt5:{bid:99}};
 const latest={key:'a',time_ms:1250,usdt_usd:1,binance:{ask:110},mt5:{bid:102}};
 const group={key:'a',status:'open',contract:100,costs:{usdt_usd:1,binance_taker_percent:1,mt5_commission_per_lot_side:100,binance_rebate_percent:30,mt5_rebate_percent:50},
   valuation:{gross:10,fees:10,carry:1,rebate_fee_base_binance:6,rebate_fee_base_mt5:4,remaining:{binance:1,mt5:1}}};
 const v=markGroups([group],basis,latest)[0].valuation;
 assert.equal(v.gross,3);assert.equal(v.estimated_exit_fee,2.1);
 assert.equal(v.estimated_rebate_binance,2.13);assert.equal(v.estimated_rebate_mt5,2.5);
 assert.ok(Math.abs(v.net_before_rebate-(-8.1))<1e-9);
 assert.ok(Math.abs(v.net-(-3.47))<1e-9);
 assert.equal(group.valuation.net,undefined);
});
