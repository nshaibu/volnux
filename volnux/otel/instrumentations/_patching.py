"""Applying and undoing the instrumentation of a method.

Every ``patch_*`` function of this package goes through :func:`patch_attr`, so that

* patching twice never wraps twice (a second ``initialize_otel`` or a test that
  patches again would otherwise produce a span per layer of wrapping);
* the original is kept and :func:`unpatch_all` puts every class back as it was
  (tests, and shutting instrumentation down);
* a subclass is not mistaken for "already patched" because a base class is (the
  old ``hasattr(cls, "_original_run")`` test made every subclass of an
  instrumented class look instrumented, so a concrete flow was never patched);
* ``classmethod`` / ``staticmethod`` keep their kind.
"""

import threading
from typing import Any, Callable, Dict, List, Tuple

__all__ = ["patch_attr", "unpatch_all", "is_patched", "patched_count"]

_lock = threading.RLock()
# (owner, name) -> (had its own attribute, original descriptor, installed descriptor)
_applied: Dict[Tuple[type, str], Tuple[bool, Any, Any]] = {}
_order: List[Tuple[type, str]] = []

_MISSING = object()


def _unwrap(descriptor: Any) -> Tuple[Callable[..., Any], Callable[[Any], Any]]:
    """The plain function inside a descriptor, and how to put a function back."""
    if isinstance(descriptor, classmethod):
        return descriptor.__func__, classmethod
    if isinstance(descriptor, staticmethod):
        return descriptor.__func__, staticmethod
    return descriptor, lambda function: function


def patch_attr(
    owner: type,
    name: str,
    make_wrapper: Callable[[Callable[..., Any]], Callable[..., Any]],
) -> bool:
    """Replace ``owner.name`` with ``make_wrapper(original)``.

    :param make_wrapper: gets the original function and returns the function that
        replaces it. It must return a coroutine function if the original is one
        (use :func:`volnux.otel.instrumentations._common.traced`).
    :return: True if the attribute was patched, False if it already was by us.
    :raises AttributeError: ``owner`` has no such attribute.
    """
    with _lock:
        key = (owner, name)
        if key in _applied:
            return False

        had_own = name in owner.__dict__
        original = owner.__dict__[name] if had_own else getattr(owner, name, _MISSING)
        if original is _MISSING:
            raise AttributeError(f"{owner.__name__} has no attribute {name!r}")
        if not had_own:
            # inherited: patch it on this class (and take it off again after)
            original = owner.__dict__.get(name, _inherited_descriptor(owner, name))

        function, rewrap = _unwrap(original)
        installed = rewrap(make_wrapper(function))
        setattr(owner, name, installed)
        _applied[key] = (had_own, original, installed)
        _order.append(key)
        return True


def _inherited_descriptor(owner: type, name: str) -> Any:
    for klass in owner.__mro__[1:]:
        if name in klass.__dict__:
            return klass.__dict__[name]
    return getattr(owner, name)


def is_patched(owner: type, name: str) -> bool:
    """Whether ``patch_attr`` has patched ``owner.name`` itself."""
    return (owner, name) in _applied


def patched_count() -> int:
    return len(_applied)


def unpatch_all() -> int:
    """Put back every attribute patched through :func:`patch_attr`; returns how many."""
    with _lock:
        count = 0
        for key in reversed(_order):
            entry = _applied.pop(key, None)
            if entry is None:
                continue
            owner, name = key
            had_own, original, installed = entry
            if owner.__dict__.get(name) is not installed:
                # something else replaced it since; do not clobber that
                continue
            if had_own:
                setattr(owner, name, original)
            else:
                delattr(owner, name)
            count += 1
        _order.clear()
        return count
