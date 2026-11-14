"""Broker I/O only. Workers never read or mutate engine/journal state."""
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event


def wall_ms(): return int(time.time()*1000)


def not_dispatched(code, error='', clock_ms=wall_ms):
    return dict(result=dict(status='done',qty=0,price=0,submitted=False,
                            error_code=code,error_kind='local_validation',error=error),
                response_received_ms=clock_ms(),received_monotonic=time.monotonic())


def dispatch_order(broker, order, started=None, ready=None, clock_ms=wall_ms):
    started=time.monotonic() if started is None else started
    dispatched=clock_ms()
    order['dispatch_ms']=dispatched
    try: result=broker.submit(order)
    except Exception as exc: result=dict(status='unknown',qty=0,price=0,error=str(exc))
    received=time.monotonic()
    return dict(result=result,dispatch_ms=dispatched,dispatch_monotonic=started,
                request_ms=round((received-started)*1000),
                response_received_ms=clock_ms(),received_monotonic=received,
                **({'dispatch_queue_ms':round((started-ready)*1000,3)} if ready is not None else {}))


def dispatch_pair(broker, orders, ready, clock_ms=wall_ms, can_start=None):
    """Release both workers only after both tasks exist; yield receipts as ready.

    Adapter timeouts remain authoritative. A future timeout must never leave a
    live write running while the engine starts another close of that position.
    """
    gate=Event();abort=Event();futures={};abort_code='DISPATCH_NOT_STARTED'
    def send(order):
        gate.wait()
        if abort.is_set(): return not_dispatched(abort_code,clock_ms=clock_ms)
        return dispatch_order(broker,order,ready=ready,clock_ms=clock_ms)
    try: pool=ThreadPoolExecutor(max_workers=2,thread_name_prefix='gold-pair-close')
    except Exception as exc:
        for index in range(len(orders)): yield index,not_dispatched('DISPATCH_NOT_STARTED',str(exc),clock_ms=clock_ms)
        return
    with pool:
        try:
            for index,order in enumerate(orders): futures[pool.submit(send,order)]=index
            if can_start is not None and not can_start():
                abort_code='STALE_EXECUTION_QUOTE';abort.set()
        except Exception:
            # Neither worker may call the broker if creation of its peer fails.
            abort.set()
        finally: gate.set()
        for future in as_completed(futures): yield futures[future],future.result()
        for index in set(range(len(orders)))-set(futures.values()):
            yield index,not_dispatched(abort_code,clock_ms=clock_ms)
