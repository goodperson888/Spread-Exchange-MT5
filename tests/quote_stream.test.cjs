const {test}=require('node:test');
const assert=require('node:assert/strict');
const {QuoteBuffer,renderPoints,markGroups}=require('../quote-stream.js');
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

test('open group mark-to-market moves with the latest quote while closed group stays fixed',()=>{
 const basis={key:'a',time_ms:1000,usdt_usd:1,binance:{ask:100},mt5:{bid:99}};
 const latest={key:'a',time_ms:1250,usdt_usd:1,binance:{ask:101},mt5:{bid:100}};
 const open={id:'open',key:'a',status:'open',contract:100,qty:1,costs:{usdt_usd:1,binance_taker_percent:0,mt5_commission_per_lot_side:0},valuation:{gross:-1,fees:0,carry:0,estimated_exit_fee:1,net:-2,remaining:{binance:1,mt5:1}}};
 const closed={id:'closed',key:'a',status:'closed',valuation:{gross:7,net:6}};
 const rows=markGroups([open,closed],basis,latest);
 assert.equal(rows[0].valuation.gross,-1);
 assert.equal(rows[0].valuation.net,-1);
 assert.deepEqual(rows[1],closed);
});
