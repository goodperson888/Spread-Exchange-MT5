'use strict';
(() => {
  const tabs=Array.from(document.querySelectorAll('[data-view]'));
  const scrolls=new Map();let selected=null;
  function select(view, focus=false) {
    if(!['trading','settings','strategy','records'].includes(view))return;
    if(selected)scrolls.set(selected,window.scrollY||0);
    for(const tab of tabs) {
      const active=tab.dataset.view===view;
      tab.setAttribute('aria-selected',String(active));tab.tabIndex=active?0:-1;
      document.getElementById('view-'+tab.dataset.view).hidden=!active;
      if(active&&focus)tab.focus();
    }
    window.dispatchEvent(new Event('gold-workspace-changed'));
    if(selected!==view)window.scrollTo?.(0,scrolls.get(view)||0);
    selected=view;
    try{localStorage.setItem('gold-pair-workspace',view);}catch(_){}
  }
  for(const [index,tab] of tabs.entries()) {
    tab.addEventListener('click',()=>select(tab.dataset.view));
    tab.addEventListener('keydown',event=>{
      const next=['ArrowRight','ArrowDown'].includes(event.key)?(index+1)%tabs.length:['ArrowLeft','ArrowUp'].includes(event.key)?(index+tabs.length-1)%tabs.length:event.key==='Home'?0:event.key==='End'?tabs.length-1:null;
      if(next===null)return;event.preventDefault();select(tabs[next].dataset.view,true);
    });
  }
  for(const button of document.querySelectorAll('[data-open-view]'))
    button.addEventListener('click',()=>select(button.dataset.openView,true));
  try{select(localStorage.getItem('gold-pair-workspace')||'trading');}catch(_){}
})();
