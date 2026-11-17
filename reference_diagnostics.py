"""Immutable process snapshots for exports; no trading locks, disk or platform I/O."""
import copy
import math


class ReferenceDiagnostics:
    def __init__(self):
        self.chart=[]
        self.commissions={}

    def set_chart(self, entries):
        result=[]
        for entry in entries[-8:]:
            item={k:v for k,v in entry.items() if k in (
                'checked_ms','start_ms','end_ms','display_offset_ms','paired_count')
                and isinstance(v,(int,float)) and not isinstance(v,bool) and math.isfinite(v)}
            for k,allowed in [('state',('error','ready')),('period',('M1','M5','M15')),
                              ('stage',('initialize','clock','rates','exchange','align','complete')),
                              ('error_code',('unconfirmed_clock','missing_time','inconsistent_clocks','clock_jump',
                                             'unstable_offset','ticks_not_advancing','bar_clock_mismatch','fetch_failed')),
                              ('clock_state',('utc_compatible','shared_advancing'))]:
                if entry.get(k) in allowed:item[k]=entry[k]
            for key in ('clock_samples','legs'):
                rows=[]
                for row in entry.get(key,[])[:18]:
                    clean={k:v for k,v in row.items() if k in ('observed_ms','tick_time_ms','offset_ms','bar_count','current_bar_ms')
                           and isinstance(v,(int,float)) and not isinstance(v,bool) and math.isfinite(v)}
                    for flag in ('tick_time_valid','observed_time_valid'):
                        if type(row.get(flag)) is bool:clean[flag]=row[flag]
                    if row.get('leg') in ('binance','mt5'):clean['leg']=row['leg']
                    rows.append(clean)
                if rows:item[key]=rows
            result.append(item)
        self.chart=result

    def snapshot(self):
        return dict(chart_reference=copy.deepcopy(self.chart),mt5_commission_reference=copy.deepcopy(self.commissions))


PROCESS_REFERENCE_DIAGNOSTICS=ReferenceDiagnostics()
