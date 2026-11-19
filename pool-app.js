'use strict';
(() => {
  const el=id=>document.getElementById(id),escape=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  let current=null,draft=null,dirty=false,working=false;
  const pushPanel=el('push-setup').closest('details'),pushHome=pushPanel.parentNode,pushNext=pushPanel.nextSibling;
  const enabled=()=>current?.pair?.mode==='pool';
  const clone=x=>JSON.parse(JSON.stringify(x));
  const pairs=rows=>{const sorted=[...rows].sort((a,b)=>a.id<b.id?-1:a.id>b.id?1:0);return sorted.flatMap((a,i)=>sorted.slice(i+1).map(b=>({id:[a.id,b.id].join('~'),a,b})));};
  const defaults=()=>({entry_spread_usd:current.strategy.entry_spread_usd,entry_lower_usd:current.strategy.entry_lower_usd??-5,
    direction_mode:'auto',target_mode:current.strategy.target_mode,take_contraction_usd:current.strategy.take_contraction_usd,
    exit_spread_usd:current.strategy.exit_spread_usd,require_net_profit:current.strategy.require_net_profit,
    min_net_profit_usd:current.strategy.min_net_profit_usd,group_loss_enabled:current.strategy.group_loss_enabled,
    group_max_loss_usd:current.strategy.group_max_loss_usd,max_hold_minutes:current.strategy.max_hold_minutes});
  const note=(text,error=false)=>{el('pool-settings-result').textContent=text;el('pool-settings-result').classList.toggle('error',error);};
  function change(){dirty=true;note('有未保存的终端或独立规则修改；点击“保存终端与独立规则”。');}
  function field(label,key,value,{type='number',min='',max='',step='any'}={}) {
    return `<label>${label}<input data-pool-field="${key}" type="${type}" value="${escape(value)}" ${min!==''?`min="${min}"`:''} ${max!==''?`max="${max}"`:''} step="${step}" spellcheck="false"></label>`;
  }
  function select(label,key,value,options){return `<label>${label}<select data-pool-rule="${key}">${Object.entries(options).map(([v,t])=>`<option value="${v}"${String(value)===v?' selected':''}>${t}</option>`).join('')}</select></label>`;}
  function renderEditors() {
    if(!draft)return;
    const expanded=new Set([...el('pool-rule-editors').querySelectorAll('details[open]')].map(x=>x.dataset.poolEdge));
    const selected=new Set([...el('pool-bulk-targets').querySelectorAll('input:checked')].map(x=>x.value));
    const source=el('pool-bulk-source').value;
    el('pool-endpoint-editors').innerHTML=draft.endpoints.map(e=>`<article class="pool-endpoint-card" data-pool-endpoint="${escape(e.id)}"><div class="heading"><h3>${escape(e.name)}</h3></div>`+
      field('终端名称','name',e.name,{type:'text'})+field('Windows MT5 完整路径','terminal_path',e.terminal_path,{type:'text'})+
      '<div class="grid">'+field('目标账号（可先检查读取）','account',e.account,{type:'text'})+field('服务器（可先检查读取）','server',e.server,{type:'text'})+
      field('完整品种名','symbol',e.symbol,{type:'text'})+field('本终端最大持仓手数','max_lots',e.max_lots,{min:.000001})+'</div>'+
      '<details><summary>本终端费用与返佣</summary><div class="grid">'+field('备用佣金 USD/手/单边','commission',e.commission,{min:0})+field('预计佣金返还 %','rebate',e.rebate,{min:0,max:100})+
      (current.execution.mode==='paper'?field('纸面 Swap 成本 USD/手/日','swap',e.swap):'')+
      `</div><label class="check"><input data-pool-field="fee_auto" type="checkbox"${e.fee_auto?' checked':''}>后台学习真实成交佣金参考</label><p class="note">成交后按实际手续费核验；未成交的平仓费仍需估算。返佣比例由你填写，参与净收益判断。</p></details>`+
      `<div class="pool-endpoint-actions"><button type="button" data-pool-check="${escape(e.id)}">检查此终端</button>${draft.endpoints.length>2?`<button type="button" data-pool-remove="${escape(e.id)}">移除终端</button>`:''}</div><p class="pool-check-result" data-pool-check-result="${escape(e.id)}" role="status">保存后检查此终端的账户、品种及合约规格。</p></article>`).join('');
    const list=pairs(draft.endpoints);const ids=new Set(list.map(x=>x.id));
    el('pool-size').textContent=`${draft.endpoints.length} 个终端 · ${list.length} 个固定终端对 · ${list.length*2} 个方向`;
    el('pool-bulk-source').replaceChildren(...list.map(({id,a,b})=>new Option(`${a.name} × ${b.name}`,id)));
    if(ids.has(source))el('pool-bulk-source').value=source;
    el('pool-bulk-targets').innerHTML=list.map(({id,a,b})=>`<label class="check"><input type="checkbox" value="${escape(id)}"${selected.has(id)?' checked':''}>${escape(a.name)} × ${escape(b.name)}</label>`).join('');
    bulkCount();
    for(const key of Object.keys(draft.rules))if(!ids.has(key))delete draft.rules[key];
    el('pool-rule-editors').innerHTML=list.map(({id,a,b})=>{
      const r=draft.rules[id]??=defaults();
      const number=(label,key)=>field(label,key,r[key]).replace('data-pool-field','data-pool-rule');
      return `<details class="pool-rule" data-pool-edge="${escape(id)}"${expanded.has(id)?' open':''}><summary>${escape(a.name)} × ${escape(b.name)} · 上限 ${escape(r.entry_spread_usd)} / 下限 ${escape(r.entry_lower_usd)}</summary><p class="note">A = ${escape(a.name)}，B = ${escape(b.name)}。正向卖 A 买 B，A Bid − B Ask ≥ 上限；反向买 A 卖 B，A Ask − B Bid ≤ 下限。</p><div class="grid">`+
        select('允许方向','direction_mode',r.direction_mode,{auto:'自动双向',forward:'只卖 A 买 B',reverse:'只买 A 卖 B'})+
        number('正向开仓上限（A−B）','entry_spread_usd')+number('反向开仓下限（A−B）','entry_lower_usd')+
        select('止盈方式','target_mode',r.target_mode,{contraction:'相对实际开仓价差收窄',absolute:'绝对 A−B 退出阈值'})+
        number('收窄目标 USD/报价单位','take_contraction_usd')+number('绝对退出价差 A−B','exit_spread_usd')+
        select('平仓净收益门槛','require_net_profit',r.require_net_profit,{true:'要求达到最低净收益',false:'不要求净收益'})+
        number('最低净收益 USD','min_net_profit_usd')+select('单组亏损保护','group_loss_enabled',r.group_loss_enabled,{false:'关闭',true:'开启'})+
        number('最大单组亏损 USD','group_max_loss_usd')+number('最长持仓分钟，0 关闭','max_hold_minutes')+'</div></details>';
    }).join('');
  }
  function configure(config) {
    if(current?.pair?.id!==config.pair?.id){el('pool-bulk-targets').replaceChildren();el('pool-bulk-source').replaceChildren();}
    current=config;
    const on=enabled();el('pool-settings').hidden=!on;el('pool-opportunities').hidden=!on;
    el('pool-chart-controls').hidden=true;
    el('check-mt5').closest('section').hidden=on;
    el('risk-heading').closest('details.risk-shell').hidden=on;
    if(!on){
      if(pushPanel.parentNode!==pushHome)pushHome.insertBefore(pushPanel,pushNext);
      draft=null;dirty=false;
      for(const id of ['direction-mode','entry','entry-lower','take','target-mode','absolute-exit','require-net','min-net','group-loss-on','group-loss','max-hold','mt5-fee','auto-mt5-fee','mt5-rebate','mt5-swap'])el(id).closest('label').hidden=false;
      el('apply-entry').hidden=false;el('grid-enabled').disabled=false;el('exit-mode').disabled=false;
      return;
    }
    draft=clone(config.mt5_pool);dirty=false;draft.rules??={};renderEditors();
    el('pool-settings').append(pushPanel);
    el('mt5-push-help').textContent='EA 只推送行情，不下单。把同一份 .set 参数分别载入每个终端的已配置黄金图表；报价按账号、服务器及品种隔离。新报价直接唤醒对应读行情会话，未接入的终端独立轮询。';
    el('pair-active-detail').textContent='多 MT5 · 同品种择优 · 原终端平仓';
    el('pair-direction').textContent=`${draft.endpoints.length} 个 MT5 · 黄金报价择优 · 两腿相同手数`;
    el('exchange-heading').textContent='终端池执行模式';
    el('setup-heading').textContent='逐个核对终端与独立组合规则';
    el('pair-parameter-scope').textContent='以下每组手数与全池额度共用；开仓、退出条件及费用在连接设置中按固定终端对维护。已有组保留原规则。';
    for(const id of ['direction-mode','entry','entry-lower','take','target-mode','absolute-exit','require-net','min-net','group-loss-on','group-loss','max-hold']) {
      const x=el(id);if(x)x.closest('label').hidden=true;
    }
    el('apply-entry').hidden=true;
    el('entry-preflight').textContent='各终端对的报价、账户核验、额度与阻拦原因见候选表；开仓前只读内存检查。';
    el('grid-enabled').checked=false;el('grid-enabled').disabled=true;
    el('exit-mode').value='group';el('exit-mode').disabled=true;
    el('native-pair-costs').hidden=true;
    for(const id of ['mt5-fee','auto-mt5-fee','mt5-rebate','mt5-swap'])el(id).closest('label').hidden=true;
    el('connection-notice').textContent='检查终端不下单。每个 MT5 使用独立路径和账户；先保存并检查，再连接终端池及对账。';
    note('终端及规则已读取。连接后持续核对账户风险、成交与佣金；更改后须保存并重新连接。');
    preview();
  }
  function preview(){
    if(!enabled())return false;
    el('plan-result').textContent=`每组两个终端各 ${el('lots').value} 手；全池最多 ${el('max-groups').value} 组，总手数额度 ${el('max-total').value}。连接时核验各终端步长、合约大小及 USD 盈亏。`;
    return true;
  }
  async function save(){
    if(!enabled()||!dirty)return;
    const submitted=JSON.stringify(draft),pool=clone(draft);
    const result=await api('/api/mt5/pool/save',{expected_pair_id:current.pair.id,pool});
    if(JSON.stringify(draft)!==submitted)throw Error('保存期间终端或规则又有修改，请核对后重新保存。');
    current=result.config;draft=clone(current.mt5_pool);dirty=false;
    note('已保存终端和独立规则。检查各终端后，重新连接使新参数生效；已有组保留原规则。');
  }
  async function work(fn){
    if(working||window.goldPairUiBusy)return;
    working=true;setUiBusy(1);
    const buttons=[...el('pool-settings').querySelectorAll('button')],disabled=buttons.map(b=>b.disabled);buttons.forEach(b=>b.disabled=true);
    const inputs=[...el('pool-settings').querySelectorAll('input,select')];inputs.forEach(x=>x.disabled=true);
    try{await saveQueue;await fn();}catch(e){note(e.message,true);}finally{working=false;setUiBusy(-1);inputs.forEach(x=>x.disabled=false);buttons.forEach((b,i)=>b.disabled=disabled[i]);}
  }
  el('pool-settings').addEventListener('input',event=>{
    const input=event.target,key=input.dataset.poolField||input.dataset.poolRule;
    if(!key)return;
    const endpoint=input.closest('[data-pool-endpoint]'),edge=input.closest('[data-pool-edge]');
    const target=endpoint?draft.endpoints.find(e=>e.id===endpoint.dataset.poolEndpoint):draft.rules[edge.dataset.poolEdge];
    target[key]=input.type==='checkbox'?input.checked:input.tagName==='SELECT'?(['true','false'].includes(input.value)?input.value==='true':input.value):input.type==='number'?(input.value.trim()===''?null:Number(input.value)):input.value;
    change();
  });
  el('pool-add').onclick=()=>{
    let id=1;while(draft.endpoints.some(e=>e.id==='t'+id))id++;
    draft.endpoints.push({id:'t'+id,name:'MT5 '+id,terminal_path:'',account:'',server:'',symbol:'XAUUSD',max_lots:1,commission:0,rebate:0,swap:0,fee_auto:true});
    renderEditors();change();
  };
  function bulkCount(){el('pool-bulk-count').textContent=`已选 ${el('pool-bulk-targets').querySelectorAll('input:checked').length} 个终端对`;}
  el('pool-bulk-targets').addEventListener('change',bulkCount);
  el('pool-bulk-all').onclick=()=>{for(const x of el('pool-bulk-targets').querySelectorAll('input'))x.checked=true;bulkCount();};
  el('pool-bulk-none').onclick=()=>{for(const x of el('pool-bulk-targets').querySelectorAll('input'))x.checked=false;bulkCount();};
  el('pool-bulk-edit').onclick=()=>{
    const source=el('pool-bulk-source').value;
    const editor=[...el('pool-rule-editors').querySelectorAll('[data-pool-edge]')].find(x=>x.dataset.poolEdge===source);
    if(editor){el('pool-individual').open=true;editor.open=true;editor.scrollIntoView({block:'start'});editor.querySelector('input,select')?.focus({preventScroll:true});}
  };
  el('pool-bulk-apply').onclick=()=>{
    const source=el('pool-bulk-source').value,rule=draft.rules[source];
    const targets=[...el('pool-bulk-targets').querySelectorAll('input:checked')].map(x=>x.value);
    if(!targets.length){note('请先勾选需要统一参数的终端对。',true);return;}
    if(!rule){note('请选择有效的参数来源。',true);return;}
    for(const key of targets)draft.rules[key]=clone(rule);
    renderEditors();dirty=true;
    note(`已将来源参数复制到 ${targets.length} 个所选终端对，可继续单独微调。点击“保存终端与独立规则”保存，再重新连接后用于新开仓；已有组保留原规则。`);
  };
  el('pool-save').onclick=()=>work(save);
  el('pool-endpoint-editors').addEventListener('click',event=>{
    const remove=event.target.closest('[data-pool-remove]'),check=event.target.closest('[data-pool-check]');
    if(remove&&draft.endpoints.length>2){draft.endpoints=draft.endpoints.filter(e=>e.id!==remove.dataset.poolRemove);renderEditors();change();}
    if(check)work(async()=>{
      await save();const id=check.dataset.poolCheck;
      const result=await api('/api/mt5/pool/check',{expected_pair_id:current.pair.id,endpoint_id:id});
      const r=result.result,output=el('pool-endpoint-editors').querySelector(`[data-pool-check-result="${id}"]`);
      const s=r.symbol;output.textContent=[r.message,r.identity?`账户 ${r.identity.account} · ${r.identity.server} · ${r.identity.currency}`:'',s?`${s.name}：每手 ${s.contract_size_oz}；最小/步长 ${s.volume_min}/${s.volume_step}`:'',...(r.blockers||[]),...(r.warnings||[])].filter(Boolean).join('\n');
      output.classList.toggle('error',!result.applied);
      if(result.applied){current=result.config;draft=clone(current.mt5_pool);
        const card=output.closest('[data-pool-endpoint]'),e=draft.endpoints.find(x=>x.id===id);
        for(const key of ['account','server'])card.querySelector(`[data-pool-field="${key}"]`).value=e[key];
        note('此终端只读检查完成，已应用账户身份。请继续检查其余终端，再连接终端池。');}
    });
  });
  function status(result){
    if(!enabled())return;
    el('parameter-scope').textContent='每组固定绑定原终端；新参数保存并重新连接后用于后续开仓。修改已有组请到记录页调整退出规则。';
    el('entry-preflight').textContent='候选表显示各方向的阈值、账户核验和阻拦原因；已有组不随最优候选切换终端。';
    if(el('config-pending')&&!el('config-pending').hidden)el('config-pending').textContent='终端池参数已保存，重新连接后用于后续开仓；已有组继续使用原终端及锁定规则。';
    preview();
    const pool=result.pool;el('pool-chart-controls').hidden=!pool;
    if(!pool){el('pool-candidate-summary').textContent='连接终端池后显示';return;}
    el('live-entry-label').textContent='图表终端对 · 正向入场';el('live-exit-label').textContent='图表终端对 · 反向入场';
    const fmt=x=>Number.isFinite(x)?Number(x).toLocaleString('zh-CN',{maximumFractionDigits:4}):'—';
    const select=el('pool-chart-pair'),signature=JSON.stringify(pool.pairs);
    if(select.dataset.signature!==signature){select.dataset.signature=signature;select.replaceChildren(...pool.pairs.map(p=>new Option(p.name,p.id)));}
    select.value=pool.chart_pair;
    el('chart-heading').textContent='固定终端对价差 · '+(pool.pairs.find(p=>p.id===pool.chart_pair)?.name||'');
    el('pool-terminal-status').innerHTML=pool.endpoints.map(e=>{
      const book=e.quote,age=book?Date.now()-Number(book.observed_ms||book.time_ms):Infinity;
      const valid=age>=-1000&&age<=Number(result.strategy_runtime?.max_quote_age_ms||3000);
      const risk=e.risk?.message||'等待账户与保证金快照';
      return `<article class="pool-status-card" data-state="${e.error||!valid?'error':'ready'}"><strong>${escape(e.name)} · ${escape(e.symbol)}</strong><p>Bid ${fmt(book?.bid)} / Ask ${fmt(book?.ask)} · ${valid?Math.max(0,age)+' ms':'报价待更新'}</p><p>剩余终端额度 ${fmt(e.remaining_lots)} 手</p><p>${escape(e.error||risk)}</p>${e.risk?.margin_level!=null?`<p>保证金水平 ${fmt(e.risk.margin_level)}% · 强平线 ${fmt(e.risk.margin_so_so)}${e.risk.margin_so_mode===0?'%':'（金额）'}</p>`:''}</article>`;
    }).join('');
    el('pool-candidate-rows').innerHTML=pool.candidates.map(row=>`<tr data-selected="${row.selected}"><td>${escape(row.buy)} → ${escape(row.sell)}</td><td>${fmt(row.spread)}</td><td>≥ ${fmt(row.threshold)}</td><td>${fmt(row.score)}</td><td>${row.selected?'优先候选 · ':''}${escape(row.reason)}</td></tr>`).join('');
    const ready=pool.candidates.filter(x=>x.ready).length;
    el('pool-candidate-summary').textContent=`${ready} / ${pool.candidates.length} 个方向满足候选条件${result.state?.enabled?'':' · 新开仓暂停'}`;
    const errors=pool.endpoints.filter(e=>e.error);
    el('risk-summary').textContent=errors.length?errors.map(e=>e.name+'：'+e.error).join(' · '):'各终端风险见候选组合卡片';
  }
  el('pool-chart-pair').onchange=async()=>{
    const button=el('pool-chart-pair');button.disabled=true;
    try{await api('/api/mt5/pool/chart',{edge:button.value});window.dispatchEvent(new Event('gold-pair-chart-changed'));}
    catch(e){el('pool-candidate-summary').textContent=e.message;}finally{button.disabled=false;}
  };
  window.GoldPairPool={configure,enabled,preview,status,save,hasDraft:()=>dirty};
})();
