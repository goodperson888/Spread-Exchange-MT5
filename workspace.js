'use strict';
(() => {
  const tabs=Array.from(document.querySelectorAll('[data-view]'));
  function select(view, focus=false) {
    if(!['trading','settings','records'].includes(view))return;
    for(const tab of tabs) {
      const active=tab.dataset.view===view;
      tab.setAttribute('aria-selected',String(active));tab.tabIndex=active?0:-1;
      document.getElementById('view-'+tab.dataset.view).hidden=!active;
      if(active&&focus)tab.focus();
    }
    window.dispatchEvent(new Event('gold-workspace-changed'));
  }
  for(const [index,tab] of tabs.entries()) {
    tab.addEventListener('click',()=>select(tab.dataset.view));
    tab.addEventListener('keydown',event=>{
      const next=event.key==='ArrowRight'?(index+1)%tabs.length:event.key==='ArrowLeft'?(index+tabs.length-1)%tabs.length:event.key==='Home'?0:event.key==='End'?tabs.length-1:null;
      if(next===null)return;event.preventDefault();select(tabs[next].dataset.view,true);
    });
  }
  for(const button of document.querySelectorAll('[data-open-view]'))
    button.addEventListener('click',()=>select(button.dataset.openView,true));
})();
