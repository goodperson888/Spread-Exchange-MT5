'use strict';
(() => {
  const el=id=>document.getElementById(id);
  let config=null;
  const id=()=>config?.pair?.exchange||'binance';
  function label(input,text) {
    const node=el(input)?.closest('label');
    const t=[...(node?.childNodes||[])].find(n=>n.nodeType===3&&n.textContent.trim());
    if(t)t.textContent=text;
  }
  function visibility(live) {
    const lighter=id()==='lighter',native=config?.pair?.type==='mt5_mt5';
    el('lighter-keys-section').hidden=!lighter||native;
    el('lighter-private-key').closest('label').hidden=!live;
    el('lighter-key-status').hidden=!live;
    el('binance-proxy-tools').hidden=lighter;
    el('public-ip-result').hidden=lighter;
    el('binance-help').hidden=lighter;
    el('lighter-help').hidden=!lighter||native;
    el('api-keys-section').style.display=live&&!lighter&&!native?'grid':'none';
  }
  function configure(c) {
    config=c;
    const same=c.pair?.type==='mt5_mt5',lighter=id()==='lighter',name=lighter?'Lighter':'币安',currency=lighter?'USDC':'USDT';
    const a=same?c.pair.short_symbol:c.symbol,b=c.mt5.symbol;
    el('chart-heading').textContent=`实时价差 · ${same?a:name+' '+a} × ${b}`;
    if(same)return visibility(c.execution.mode==='live');
    el('exchange-heading').textContent=name+'连接配置';
    el('exchange-settings').querySelector('.eyebrow').textContent='02 · '+name+'永续';
    el('check-binance').textContent='检查'+name+'连接';
    el('pair-active-detail').textContent=`${name} ${currency} 永续 × MT5 · 黄金数量配平`;
    el('pair-direction').textContent=`卖 ${name} ${a} · 买 MT5 ${b}`;
    el('market-overview').querySelectorAll('small')[0].textContent=`卖 ${name} Bid − 买 MT5 Ask`;
    el('market-overview').querySelectorAll('small')[1].textContent=`买回 ${name} Ask − 卖 MT5 Bid`;
    el('entry-leg').options[0].textContent=`${name}优先（先卖 ${name}，再买 MT5）`;
    el('entry-leg').options[1].textContent=`MT5 优先（先买 MT5，再卖 ${name}）`;
    label('bid',`${name} Bid（${currency}）`);label('binance-ask',`${name} Ask（${currency}）`);
    label('auto-fx',`自动获取 ${currency}/USD 汇率`);label('usdt-fx',`${currency}/USD 汇率（备用）`);
    label('auto-binance-fee',`自动获取 ${name} 账户吃单费率`);label('binance-fee',`${name} 备用吃单费率（%）`);
    label('binance-rebate',`${name} 手续费返佣比例（%）`);label('exit-rule-binance-rebate',`本组 ${name} 手续费返佣（%）`);
    el('risk-binance-heading').textContent=name+'强平距离';
    const datalist=el('exchange-symbols');datalist.replaceChildren(...(lighter?['XAU','PAXG']:['XAUUSDT','PAXGUSDT']).map(s=>new Option(s,s)));
    visibility(c.execution.mode==='live');
  }
  window.GoldPairVenueUI={id,configure,visibility};
})();
