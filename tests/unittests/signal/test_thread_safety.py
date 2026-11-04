import threading

from .conftest import Sender


def test_concurrent_connect_disconnect_and_emit(signal):
    handlers = [(lambda **kw: None) for _ in range(50)]  # kept alive by this list
    errors = []
    stop = threading.Event()

    def churn():
        try:
            while not stop.is_set():
                for h in handlers:
                    signal.connect(Sender, h)
                for h in handlers:
                    signal.disconnect(Sender, h)
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(exc)

    def emitter():
        try:
            for _ in range(500):
                signal.emit(Sender, value=1)
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(exc)

    churners = [threading.Thread(target=churn) for _ in range(2)]
    emitters = [threading.Thread(target=emitter) for _ in range(4)]
    for t in churners + emitters:
        t.start()
    for t in emitters:
        t.join()
    stop.set()
    for t in churners:
        t.join()
    assert errors == []


def test_concurrent_connects_do_not_lose_listeners(signal):
    handlers = [(lambda **kw: None) for _ in range(100)]
    barrier = threading.Barrier(len(handlers))

    def connect(h):
        barrier.wait()
        signal.connect(Sender, h)

    threads = [threading.Thread(target=connect, args=(h,)) for h in handlers]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(signal.emit(Sender, value=1)) == len(handlers)
