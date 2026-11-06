/* Bounded immutable client sample buffer; transport cadence is independent of paint cadence. */
(function(root) {
  function sameChartMarket(a,b) {
    if(a===b)return true;
    const left=String(a||'').split('|'),right=String(b||'').split('|');
    return left.length===9&&right.length===9&&left.every((value,i)=>i===4||value===right[i]);
  }
  class QuoteBuffer {
    constructor(capacity=100000) {this.capacity=capacity;this.points=new Map();this.rows=[];this.key=null;}
    merge(samples, since=0, key=null) {
      if(key!==this.key){
        this.rows=this.rows.filter(q=>!key||!q.key||sameChartMarket(q.key,key));
        this.points=new Map(this.rows.map(q=>[q.time_ms,q]));this.key=key;
      }
      let unordered=false,tail=this.rows.at(-1)?.time_ms??-Infinity;
      for(const q of samples) {
        if(!Number.isFinite(q.time_ms)||!Number.isFinite(q.entry)||!Number.isFinite(q.exit)||q.time_ms<since)continue;
        if(key && q.key && !sameChartMarket(q.key,key))continue;
        if(this.points.has(q.time_ms))continue;
        if(q.time_ms<tail)unordered=true;
        tail=Math.max(tail,q.time_ms);this.points.set(q.time_ms,q);this.rows.push(q);
      }
      // Normal live batches are ordered appends. Only backfilled history needs sorting.
      if(unordered)this.rows.sort((a,b)=>a.time_ms-b.time_ms);
      let remove=Math.max(0,this.rows.length-this.capacity);
      while(remove<this.rows.length&&this.rows[remove].time_ms<since)remove++;
      if(remove){
        for(let i=0;i<remove;i++)this.points.delete(this.rows[i].time_ms);
        this.rows=this.rows.slice(remove);
      }
      return this.rows;
    }
    replaceHistory(samples,since,key) {
      const fresh=new QuoteBuffer(this.capacity);
      // Live samples already shown take precedence over repeated history rows.
      fresh.merge([...this.points.values()],since,key);
      const rows=fresh.merge(samples,since,key);this.points=fresh.points;this.rows=fresh.rows;this.key=fresh.key;return rows;
    }
  }
  // Fixed UTC buckets keep past representatives stable as the right edge grows.
  function renderPoints(rows,windowMs,view=null) {
    if(rows.length<=6000)return rows;
    const step=Math.max(1,Math.ceil(windowMs/6000));let previous=null;
    const liveStart=(rows.at(-1)?.time_ms||0)-60000;
    return rows.filter(q=>{if(q.time_ms>=liveStart)return true;const localStep=view&&q.time_ms>=view.start&&q.time_ms<=view.end?Math.max(1,Math.ceil((view.end-view.start)/6000)):step;const bucket=Math.floor(q.time_ms/localStep);if(bucket===previous)return false;previous=bucket;return true;});
  }
  function chartSeries(rows,windowMs,view=null) {
    // Detect holes before display thinning; never draw through invalid quotes.
    const intervals=[];
    for(let i=1;i<rows.length;i++)if(rows[i].time_ms>rows[i-1].time_ms)intervals.push(rows[i].time_ms-rows[i-1].time_ms);
    intervals.sort((a,b)=>a-b);
    const tolerance=Math.max(10000,Math.min(60000,intervals[Math.floor(intervals.length/2)]||0)*3);
    const gaps=[],valid=[];let previous=null,invalidStart=null;
    for(const row of rows) {
      if(row.valid===false) {invalidStart??=previous?.time_ms??row.time_ms;continue;}
      if(invalidStart!==null) {gaps.push({start:invalidStart,end:row.time_ms,reason:'报价无效或过期'});invalidStart=null;}
      else if(previous&&row.time_ms-previous.time_ms>tolerance)gaps.push({start:previous.time_ms,end:row.time_ms,reason:'本机采样缺失'});
      valid.push(row);previous=row;
    }
    if(invalidStart!==null)gaps.push({start:invalidStart,end:rows.at(-1).time_ms,reason:'报价无效或过期'});
    const samples=renderPoints(valid,windowMs,view),lines=[];let gapIndex=0;
    for(const row of samples) {
      while(gapIndex<gaps.length&&gaps[gapIndex].end<=row.time_ms){
        const gap=gaps[gapIndex++];lines.push({time_ms:gap.start+1,entry:null,exit:null});
      }
      lines.push(row);
    }
    if(gapIndex<gaps.length)lines.push({time_ms:gaps[gapIndex].start+1,entry:null,exit:null});
    return {samples,lines,gaps,tolerance};
  }
  // Display projection only; execution decisions remain in the backend.
  // Always project from one coherent server snapshot, never accumulate deltas.
  function markGroups(groups,basis,q) {
    if(!basis||!q||basis.key!==q.key||q.time_ms<basis.time_ms)return groups;
    return groups.map(g=>{
      const v=g.valuation, c=g.costs;
      if(g.status!=='open'||g.valuation_status==='unverified'||!v||!c||g.key!==q.key)return g;
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
  const api={QuoteBuffer,renderPoints,chartSeries,sameChartMarket,markGroups};
  if(typeof module!=='undefined')module.exports=api;
  else root.GoldPairQuotes=api;
})(typeof window!=='undefined'?window:globalThis);
