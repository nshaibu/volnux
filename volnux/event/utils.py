import typing

if typing.TYPE_CHECKING:
    from volnux.event import EventBase


DEFAULT_EVENT_NAMESPACE = "local"


def resolve_event_ref_to_class(
    event_ref: typing.Union[str, typing.Type["EventBase"]],
    default_namespace: str = DEFAULT_EVENT_NAMESPACE,
) -> typing.Type["EventBase"]:
    """Resolve an event string or class to an EventBase subclass.

    Accepts either:
    - A plain event name: ``"user_created"``
    - A namespaced event name: ``"payments::invoice_paid"``
    - An EventBase subclass directly, which is returned unchanged.

    :param event_ref: Event name string or an EventBase subclass.
    :param default_namespace: Namespace to use when none is specified in the
        string. Defaults to ``DEFAULT_EVENT_NAMESPACE`` (``"local"``).
    :return: The resolved EventBase subclass.
    :raises TypeError: If event_ref is neither a str nor an EventBase subclass.
    :raises ValueError: If the string is malformed or the event is not found
        in the registry.
    """
    from volnux.event import EventBase

    if isinstance(event_ref, str):
        return _resolve_from_string(event_ref, default_namespace)

    try:
        if issubclass(event_ref, EventBase):
            return event_ref
    except TypeError:
        pass  # event_ref is not a class at all; fall through to the error below

    raise TypeError(
        f"event_ref must be a str or an EventBase subclass, "
        f"got {type(event_ref).__name__!r}."
    )


def _resolve_from_string(
    event_ref: str,
    default_namespace: str,
) -> typing.Type["EventBase"]:
    """Parse a (possibly namespaced) event string and look it up in the registry."""
    from volnux.event import get_event_registry

    event_ref = event_ref.strip()
    event_version = None

    if "::" in event_ref:
        parts = event_ref.split("::", 1)
        namespace, event_name = parts[0].strip(), parts[1].strip()
        if "@" in event_name:
            event_name, event_version = event_name.split("@", 1)
    else:
        namespace = default_namespace
        event_name = event_ref

    if not namespace:
        raise ValueError(
            f"Event string {event_ref!r} has an empty namespace. "
            f"Expected format: 'namespace::event_name'."
        )
    if not event_name:
        raise ValueError(
            f"Event string {event_ref!r} has an empty event name. "
            f"Expected format: 'namespace::event_name'."
        )

    registry = get_event_registry()
    event_class = registry.get_by_name(event_name, namespace, version=event_version)

    if event_class is None:
        raise ValueError(f"Unknown event {event_name!r} in namespace {namespace!r}.")

    return event_class
