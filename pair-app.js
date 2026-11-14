'use strict';
(() => {
  const node=id=>document.getElementById(id);
  let catalogue=null,current=null,running=null,working=false;
  const native=c=>c?.pair?.type==='mt5_mt5';
  function label(id,value) {
    const parent=node(id)?.closest('label');
    if(!parent)return;
    const text=[...parent.childNodes].find(x=>x.nodeType===3&&x.textContent.trim());
    if(text)text.textContent=value;
  }
  function configure(config) {
    current=config;
    const same=native(config),a=same?config.pair.short_symbol:config.symbol,b=config.mt5.symbol;
    node('pair-active-name').textContent=config.pair?.name||`${a} × ${b}`;
    node('pair-active-detail').textContent=same?'同一 MT5 · 双腿相同手数':'币安永续 × MT5 · 黄金数量配平';
    node('pair-direction').textContent=`卖 ${a} · 买 ${b} · ${same?'同一 MT5':'币安 × MT5'}`;
    node('exchange-symbol-field').hidden=same;node('exchange-network').hidden=same;
    node('exchange-heading').textContent=same?'执行模式与优先腿':'币安连接配置';
    node('exchange-settings').querySelector('.eyebrow').textContent=same?'02 · 执行设置':'02 · 币安永续';
    node('risk-heading').textContent=same?'MT5 账户保证金风险':'双边强平风险';
    const costs=node('cost-source');costs.dataset.original??=costs.textContent;
    costs.textContent=same?'实盘两腿成交佣金与 Swap 分别后台核验；预计平仓佣金按各腿每手单边设置计算，返佣按填写比例预计。纸面持仓费正值为成本、负值为收入。':costs.dataset.original;
    node('check-binance').hidden=same;node('native-pair-costs').hidden=!same;
    for(const id of ['auto-fx','usdt-fx','auto-binance-fee','binance-fee','binance-rebate','paper-funding'])node(id).closest('label').hidden=same;
    node('mt5-short-swap').closest('label').hidden=config.execution.mode==='live';
    node('market-overview').querySelectorAll('small')[0].textContent=same?'卖出腿 Bid − 买入腿 Ask':'卖币安 Bid − 买 MT5 Ask';
    node('market-overview').querySelectorAll('small')[1].textContent=same?'买回卖出腿 Ask − 卖出买入腿 Bid':'买回币安 Ask − 卖 MT5 Bid';
    node('risk-binance-heading').closest('.risk-card').hidden=same;
    for(const id of ['risk-binance-warning','risk-binance-danger','recv-window'])node(id).closest('label').hidden=same;
    label('bid',same?'卖出腿 Bid（USD）':'参考币安 Bid（USDT）');
    label('binance-ask',same?'卖出腿 Ask（USD）':'币安 Ask（USDT）');
    label('mt5-bid',same?'买入腿 Bid（USD）':'MT5 Bid（USD）');
    label('mt5-ask',same?'买入腿 Ask（USD）':'MT5 Ask（USD）');
    label('contract',same?'买入腿每手合约数量':'每手盎司');
    label('lots',same?'每组每腿手数（两腿相同）':'每组 MT5 手数');
    label('mt5-fee',same?'买入腿佣金预估（USD/手/单边）':'MT5 佣金预估（USD/手/单边）');
    label('mt5-rebate',same?'买入腿佣金返佣比例（%）':'MT5 佣金返佣比例（%）');
    label('exit-rule-binance-rebate',same?'本组卖出腿佣金返佣（%）':'本组币安手续费返佣（%）');
    label('exit-rule-mt5-rebate',same?'本组买入腿佣金返佣（%）':'本组 MT5 佣金返佣（%）');
    for(const input of document.querySelectorAll('label'))for(const text of input.childNodes) {
      if(text.nodeType===3)text.textContent=text.textContent.replace(same?'USD/盎司':'USD/报价单位',same?'USD/报价单位':'USD/盎司');
    }
    node('adoption-panel').hidden=same;
    node('entry-leg').options[0].textContent=same?'卖出腿优先（先卖 A，再买 B）':'币安优先（先卖币安，再买 MT5）';
    node('entry-leg').options[1].textContent=same?'买入腿优先（先买 B，再卖 A）':'MT5 优先（先买 MT5，再卖币安）';
    for(const [id,key] of [['mt5-short-fee','mt5_short_commission_per_lot_side'],['mt5-short-rebate','mt5_short_rebate_percent'],['mt5-short-swap','mt5_short_swap_per_lot_day']])node(id).value=config.costs[key]??0;
    // Connection settings are shared; instrument identity lives in the selected combination.
    for(const id of ['exchange-symbol-field','mt5-symbol']) {
      const input=id==='mt5-symbol'?node(id):node('symbol');
      if(input)input.disabled=Boolean(config.pair?.id&&config.pair.id!=='legacy');
    }
    syncConditionalFields();
  }
  function status(result) {
    running=result;
    const summary=node('risk-summary'),risk=result.liquidation_risk;
    if(summary) {
      const sides=result.pair?.type==='mt5_mt5'?[risk?.mt5]:[risk?.binance,risk?.mt5];
      summary.textContent=sides.map(x=>x?.message||x?.state||'等待账户快照').join(' · ');
      summary.classList.toggle('error',sides.some(x=>['danger','warning'].includes(x?.level)));
    }
    const list=node('record-pair-filter');
    if(list) {
      const selected=list.value,items=new Map();
      for(const g of result.state?.groups||[])items.set(g.pair?.id||'legacy',g.pair?.name||`${g.symbol} × ${g.mt5_symbol}`);
      const signature=JSON.stringify([...items]);
      if(list.dataset.signature!==signature) {
        list.dataset.signature=signature;
        list.replaceChildren(new Option('全部组合','all'),...[...items].map(([id,name])=>new Option(name,id)));
        list.value=items.has(selected)?selected:'all';
      }
    }
  }
  function preview() {
    const same=node('pair-template').value==='native';
    node('pair-short').readOnly=!same;
    if(!same)node('pair-short').value=node('pair-template').value==='paxg'?'PAXGUSDT':'XAUUSDT';
    node('pair-preview').textContent=`卖 ${node('pair-short').value||'待选 A'} / 买 ${node('pair-long').value||'待选 B'}\n`+
      (same?'每组两腿使用相同手数，分别校验步长；实际数量和盈亏按各自合约大小计算。相同手数可能对应不同名义金额。两个品种需为同一标的、相同报价单位。':'按 MT5 实际合约大小换算币安黄金数量，并校验交易所数量规则。');
  }
  function choose(id) {
    const p=catalogue?.profiles.find(x=>x.id===id);
    node('pair-list').value=p?.id||'';
    node('pair-name').value=p?.name||'';
    node('pair-template').value=p?.type==='mt5_mt5'?'native':p?.short_symbol==='PAXGUSDT'?'paxg':'xau';
    node('pair-short').value=p?.short_symbol||'XAUUSDT';node('pair-long').value=p?.long_symbol||current?.mt5?.symbol||'';
    node('pair-activate').disabled=!p||p.id===catalogue.active_id;
    node('pair-delete').disabled=!p||p.id===catalogue.active_id;
    preview();
  }
  async function refresh(id) {
    catalogue=await api('/api/pairs');
    node('pair-list').replaceChildren(new Option('新建组合…',''),...catalogue.profiles.map(p=>new Option(p.name+(p.id===catalogue.active_id?'（当前）':''),p.id)));
    choose(id??catalogue.active_id);
  }
  async function work(fn) {
    if(working||window.goldPairUiBusy)return;
    working=true;setUiBusy(1);busy=true;clearTimeout(saveTimer);
    const buttons=[...node('pair-manager').querySelectorAll('button')];
    const disabled=buttons.map(b=>b.disabled);buttons.forEach(b=>b.disabled=true);
    node('pair-manager-result').classList.remove('error');
    try {await saveQueue;await fn();}
    catch(error){node('pair-manager-result').textContent=error.message;node('pair-manager-result').classList.add('error');}
    finally {
      working=false;busy=false;setUiBusy(-1);buttons.forEach((b,i)=>b.disabled=disabled[i]);
      const selected=catalogue?.profiles.find(p=>p.id===node('pair-list').value);
      node('pair-activate').disabled=!selected||selected.id===catalogue?.active_id;
      node('pair-delete').disabled=!selected||selected.id===catalogue?.active_id;
    }
  }
  node('pair-manage').onclick=()=>work(async()=>{
    if(!loaded)throw Error('本机配置尚未加载，请稍后重试');
    if(dirtyFields.size||credentialsDirty)await saveConfig();
    await refresh();node('pair-manager-result').classList.remove('error');
    node('pair-manager-result').textContent='选择组合仅预览；设为当前组合后需要重新连接和对账。';
    node('pair-manager').showModal();
  });
  node('pair-manager-close').onclick=()=>node('pair-manager').close();
  node('pair-list').onchange=()=>choose(node('pair-list').value);
  node('pair-new').onclick=()=>choose('');
  node('pair-template').onchange=()=>{
    if(node('pair-template').value==='native')node('pair-short').value='';
    preview();
  };
  for(const id of ['pair-short','pair-long'])node(id).oninput=preview;
  node('pair-save').onclick=()=>work(async()=>{
    const result=await api('/api/pairs/save',{id:node('pair-list').value||undefined,name:node('pair-name').value,
      type:node('pair-template').value==='native'?'mt5_mt5':'exchange_mt5',
      short_symbol:node('pair-short').value,long_symbol:node('pair-long').value});
    const saved=result.profiles.find(x=>x.id===result.saved_id);
    if(saved?.id===result.active_id){const response=await api('/api/config');populate(response.config);}
    await refresh(saved?.id);node('pair-manager-result').textContent='组合已保存；未启动自动交易。';
  });
  node('pair-activate').onclick=()=>work(async()=>{
    const selected=node('pair-list').value;
    const p=catalogue.profiles.find(x=>x.id===selected);
    if(!p)throw Error('请先保存组合');
    if(p.name!==node('pair-name').value||p.short_symbol!==node('pair-short').value||p.long_symbol!==node('pair-long').value||
       (p.type==='mt5_mt5')!==(node('pair-template').value==='native'))throw Error('组合有未保存修改，请先保存');
    await api('/api/pairs/activate',{id:selected});
    const response=await api('/api/config');
    clearDraft();dirtyFields.clear();populate(response.config);
    await refresh(selected);node('pair-manager-result').textContent='已切换当前组合，自动交易未启动。请到连接设置检查，再连接行情与对账。';
    window.dispatchEvent(new Event('gold-pair-activated'));
  });
  node('pair-delete').onclick=()=>work(async()=>{
    await api('/api/pairs/delete',{id:node('pair-list').value});await refresh();
    node('pair-manager-result').textContent='非当前组合配置已删除，历史交易记录保留。';
  });
  window.GoldPairPairs={configure,status,native,info:()=>current?.pair,labels:g=>g?.pair?.type==='mt5_mt5'?{a:'MT5 卖出腿',b:'MT5 买入腿',unit:'手'}:{a:'币安',b:'MT5',unit:'盎司'}};
})();
