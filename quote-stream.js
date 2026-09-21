/* Bounded immutable client sample buffer; transport cadence is independent of paint cadence. */
(function(root) {
  class QuoteBuffer {
    constructor(capacity=100000) {this.capacity=capacity;this.points=new Map();}
    merge(samples, since=0, key=null) {
      for(const q of samples) {
        if(!Number.isFinite(q.time_ms)||!Number.isFinite(q.entry)||!Number.isFinite(q.exit))continue;
        if(key && q.key && q.key!==key)continue;
        if(!this.points.has(q.time_ms))this.points.set(q.time_ms,q);
      }
      let rows=[...this.points.values()].filter(q=>q.time_ms>=since&&(!key||!q.key||q.key===key)).sort((a,b)=>a.time_ms-b.time_ms);
      rows=rows.slice(-this.capacity);this.points=new Map(rows.map(q=>[q.time_ms,q]));
      return rows;
    }
    replaceHistory(samples,since,key) {
      const fresh=new QuoteBuffer(this.capacity);
      // Live samples already shown take precedence over repeated history rows.
      fresh.merge([...this.points.values()],since,key);
      const rows=fresh.merge(samples,since,key);this.points=fresh.points;return rows;
    }
  }
  // Fixed UTC buckets keep past representatives stable as the right edge grows.
  function renderPoints(rows,windowMs,view=null) {
    if(rows.length<=6000)return rows;
    const step=Math.max(1,Math.ceil(windowMs/6000));let previous=null;
    const liveStart=(rows.at(-1)?.time_ms||0)-60000;
    return rows.filter(q=>{if(q.time_ms>=liveStart)return true;const localStep=view&&q.time_ms>=view.start&&q.time_ms<=view.end?Math.max(1,Math.ceil((view.end-view.start)/6000)):step;const bucket=Math.floor(q.time_ms/localStep);if(bucket===previous)return false;previous=bucket;return true;});
  }
  // Display projection only; execution decisions remain in the backend.
  // Always project from one coherent server snapshot, never accumulate deltas.
  function markGroups(groups,basis,q) {
    if(!basis||!q||basis.key!==q.key||q.time_ms<basis.time_ms)return groups;
    return groups.map(g=>{
      const v=g.valuation, c=g.costs;
      if(g.status!=='open'||!v||!c||g.key!==q.key)return g;
      const b=Number(v.remaining?.binance||0),m=Number(v.remaining?.mt5||0);
      const fx=q.usdt_usd??c.usdt_usd, oldFx=basis.usdt_usd??c.usdt_usd;
      const gross=v.gross-b*(q.binance.ask*fx-basis.binance.ask*oldFx)+m*(q.mt5.bid-basis.mt5.bid);
      const exitBinance=b*q.binance.ask*fx*c.binance_taker_percent/100;
      const exitMt5=m/g.contract*c.mt5_commission_per_lot_side;
      const exit=exitBinance+exitMt5;
      if(![gross,exit,v.fees,v.carry].every(Number.isFinite))return g;
      return {...g,valuation:{...v,gross,estimated_exit_fee:exit,estimated_exit_fee_binance:exitBinance,estimated_exit_fee_mt5:exitMt5,net:gross-v.fees-exit+v.carry}};
    });
  }
  const api={QuoteBuffer,renderPoints,markGroups};
  if(typeof module!=='undefined')module.exports=api;
  else root.GoldPairQuotes=api;
})(typeof window!=='undefined'?window:globalThis);
