const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');

function fixture(){
 const elements=new Map(),calls=[];
 const el=id=>{
  if(!elements.has(id)){
   const element={value:'',dataset:{},innerHTML:'',textContent:'',parentNode:{insertBefore(){}},
    classList:{toggle(){}},querySelectorAll:()=>[],querySelector:()=>({}),append(){},
    replaceChildren(){},addEventListener(name,fn){this['on'+name]=fn;}};
   element.closest=()=>element;elements.set(id,element);
  }
  return elements.get(id);
 };
 const config=JSON.parse(fs.readFileSync(path.join(__dirname,'../config.example.json'),'utf8'));
 config.pair={id:'pool',mode:'pool',type:'mt5_mt5'};
 config.mt5_pool={endpoints:[{id:'a',name:'平台甲'},{id:'b',name:'平台乙'}],rules:{'a~b':{
  direction_mode:'reverse',entry_forward_confirmation_ticks:2,entry_reverse_confirmation_ticks:7,
  entry_forward_confirmation_mode:'at_or_better',entry_reverse_confirmation_mode:'equal'}}};
 const window={};
 vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../pool-app.js'),'utf8'),{
  window,document:{getElementById:el},Option:function(text,value){this.text=text;this.value=value;},
  setUiBusy(){},api:async(url,body)=>{calls.push({url,body});return {config:{...config,mt5_pool:body.pool}};}
 });
 window.GoldPairPool.configure(config);
 function edit(key,value,type='number'){
  const edge={dataset:{poolEdge:'a~b'},querySelector:()=>({})};
  const target={dataset:{poolRule:key},value,type,tagName:type==='select'?'SELECT':'INPUT',
   closest:selector=>selector==='[data-pool-edge]'?edge:null};
  el('pool-settings').oninput({target});
 }
 return {el,calls,config,edit,pool:window.GoldPairPool};
}

test('pool shared controls translate into both persisted directions and keep profit count independent',async()=>{
 const f=fixture();
 assert.match(f.el('pool-rule-editors').innerHTML,/data-pool-rule="entry_confirmation_ticks"[^>]*value="7"/);
 assert.doesNotMatch(f.el('pool-rule-editors').innerHTML,/data-pool-rule="entry_forward_confirmation_ticks"/);
 await f.pool.save();assert.equal(f.calls.length,0);
 f.edit('entry_confirmation_ticks','5');f.edit('entry_confirmation_mode','at_or_better','select');
 f.edit('exit_confirmation_ticks','4');await f.pool.save();
 const rules=f.calls[0].body.pool.rules['a~b'];
 for(const d of ['forward','reverse']){
  assert.equal(rules[`entry_${d}_confirmation_ticks`],5);
  assert.equal(rules[`entry_${d}_confirmation_mode`],'at_or_better');
 }
 assert.equal(rules.exit_confirmation_ticks,4);
 assert.equal(rules.entry_confirmation_ticks,undefined);assert.equal(rules.entry_confirmation_mode,undefined);
 assert.equal(f.config.mt5_pool.rules['a~b'].entry_forward_confirmation_ticks,2);
 assert.equal(f.pool.hasDraft(),false);
});

test('pool legacy confirmation selection survives changing allowed buy/sell combinations before save',async()=>{
 const f=fixture();f.edit('direction_mode','auto','select');await f.pool.save();
 const r=f.calls[0].body.pool.rules['a~b'];
 assert.equal(r.direction_mode,'auto');assert.equal(r.entry_forward_confirmation_ticks,7);
 assert.equal(r.entry_reverse_confirmation_ticks,7);assert.equal(r.entry_forward_confirmation_mode,'equal');
});
