import logging
import operator
import re
from datetime import date
from typing import Any, Callable, Dict, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from volnux.result.stream import Q

logger = logging.getLogger(__name__)

# Maximum regex execution time in seconds (prevents ReDoS)
_REGEX_TIMEOUT_CHARS = 10_000

LOOKUP_OPERATORS: Dict[str, Callable[[Any, Any], bool]] = {
    "exact": operator.eq,
    "iexact": lambda a, b: str(a).lower() == str(b).lower() if a is not None else False,
    "contains": lambda a, b: b in str(a) if a is not None else False,
    "icontains": lambda a, b: (
        b.lower() in str(a).lower() if a is not None and isinstance(b, str) else False
    ),
    "in": lambda a, b: a in b if b is not None else False,
    "gt": lambda a, b: a > b if a is not None and b is not None else False,
    "gte": lambda a, b: a >= b if a is not None and b is not None else False,
    "lt": lambda a, b: a < b if a is not None and b is not None else False,
    "lte": lambda a, b: a <= b if a is not None and b is not None else False,
    "startswith": lambda a, b: (
        str(a).startswith(b) if a is not None and isinstance(b, str) else False
    ),
    "istartswith": lambda a, b: (
        str(a).lower().startswith(b.lower())
        if a is not None and isinstance(b, str)
        else False
    ),
    "endswith": lambda a, b: (
        str(a).endswith(b) if a is not None and isinstance(b, str) else False
    ),
    "iendswith": lambda a, b: (
        str(a).lower().endswith(b.lower())
        if a is not None and isinstance(b, str)
        else False
    ),
    "range": lambda a, b: b[0] <= a <= b[1] if a is not None and len(b) == 2 else False,
    "isnull": lambda a, b: (a is None) == b,
}


# Lookups whose operand is a value of the field's own type (not text or a flag)
_VALUE_LOOKUPS = frozenset({"exact", "gt", "gte", "lt", "lte", "in", "range"})


def _coerce_temporal(record_value: Any, lookup_type: str, value: Any) -> Any:
    """Make a filter value comparable with a stored ``datetime``/``date``.

    Stored values are real datetimes while callers often filter with an ISO string,
    an epoch number, or a datetime in the other time-zone convention. Without this
    the comparison raises and is silently reported as "no match".
    """
    if lookup_type not in _VALUE_LOOKUPS or not isinstance(record_value, date):
        return value
    from volnux.backends.fields.datetime import coerce_temporal_like

    if lookup_type in ("in", "range") and isinstance(value, (list, tuple, set)):
        return type(value)(coerce_temporal_like(record_value, v) for v in value)
    return coerce_temporal_like(record_value, value)


def _safe_regex_match(value: Any, pattern: str, ignore_case: bool = False) -> bool:
    """Regex match with length guard to prevent ReDoS."""
    if value is None:
        return False
    text = str(value)
    if len(text) > _REGEX_TIMEOUT_CHARS:
        logger.warning("Regex skipped: value exceeds %d chars", _REGEX_TIMEOUT_CHARS)
        return False
    try:
        flags = re.IGNORECASE if ignore_case else 0
        return bool(re.search(pattern, text, flags))
    except re.error as e:
        logger.warning("Invalid regex pattern '%s': %s", pattern, e)
        return False


# Register regex operators with safety wrapper
LOOKUP_OPERATORS["regex"] = lambda a, b: _safe_regex_match(a, b)
LOOKUP_OPERATORS["iregex"] = lambda a, b: _safe_regex_match(a, b, ignore_case=True)

LOOKUP_SEPARATOR = "__"


def parse_lookup(field_spec: str) -> Tuple[str, str]:
    """Parse 'field__lookup' into (field_path, lookup_type)."""
    parts = field_spec.rsplit(LOOKUP_SEPARATOR, 1)
    if len(parts) == 2 and parts[1] in LOOKUP_OPERATORS:
        return parts[0], parts[1]
    return field_spec, "exact"


_NO_ALIAS = object()
_FK_ID_SUFFIX = "_object_id"


def _foreign_key_id(record: Any, part: str) -> Any:
    """``<field>_object_id`` on a record that has a foreign key ``<field>``.

    SQL stores keep the id of a reference in a ``<field>_object_id`` column, so that
    is the name filters use for it (``OnDelete`` handling, migrations). A record held
    in memory has the reference itself under ``<field>``; this gives its id, so the
    same filter works on both. Returns ``_NO_ALIAS`` when ``part`` is not such a name.
    """
    if not part.endswith(_FK_ID_SUFFIX) or len(part) == len(_FK_ID_SUFFIX):
        return _NO_ALIAS
    name = part[: -len(_FK_ID_SUFFIX)]
    if isinstance(record, dict):
        if name not in record:
            return _NO_ALIAS
        reference = record[name]
    elif hasattr(record, name):
        reference = getattr(record, name)
    else:
        return _NO_ALIAS
    if reference is None:
        return None
    if isinstance(reference, dict):
        return reference.get("object_id", _NO_ALIAS)
    return getattr(reference, "object_id", getattr(reference, "id", _NO_ALIAS))


def resolve_field_value(record: Any, field_path: str) -> Tuple[Any, bool]:
    """Traverse nested attributes AND dict keys on a record.

    Returns (value, found). Distinguishes between 'attribute is None'
    and 'attribute does not exist' to prevent false positives.
    """
    current = record
    for part in field_path.split(LOOKUP_SEPARATOR):
        if current is None:
            return None, False

        # Try dict access first (JSONB / serialized fields)
        if isinstance(current, dict):
            if part in current:
                current = current[part]
                continue
            alias = _foreign_key_id(current, part)
            if alias is not _NO_ALIAS:
                current = alias
                continue
            return None, False

        # Try object attribute access
        if hasattr(current, part):
            current = getattr(current, part)
            continue

        alias = _foreign_key_id(current, part)
        if alias is not _NO_ALIAS:
            current = alias
            continue

        return None, False

    return current, True


def create_filter_predicate(**filter_kwargs: Any) -> Callable[[Any], bool]:
    """
    Create a combined AND predicate function from Django-style filter keyword arguments.

    This function generates a predicate function that can be used to evaluate
    whether a given record matches all specified filtering conditions. The filter
    keyword arguments follow a Django-like syntax with support for field lookups,
    such as exact matches, comparisons, substring matches, and checks against
    lists or ranges.

    :param filter_kwargs: Dictionary where keys are field lookup expressions
        (e.g., "field__lookup") and values are the values to match against.
        Supported lookup operators include:
          - exact: Matches exact values.
          - gt: Matches values greater than the provided value.
          - gte: Matches values greater than or equal to the provided value.
          - lt: Matches values less than the provided value.
          - lte: Matches values less than or equal to the provided value.
          - contains: Matches if the field contains the given substring.
          - icontains: Case-insensitive version of `contains`.
          - in: Checks if the value is in a list or iterable.
          - isnull: Checks if the field is null (value should be True or False).

    :return: A callable predicate function that receives a record as input and
        evaluates to True if the record matches all filter conditions, otherwise
        False.

    Example:
        - age__gt=25
        - name__contains="John"
        - email__icontains="gmail"
        - status__in=["active", "pending"]
        - created_at__gte=datetime(2023, 1, 1)
    """
    predicates: List[Callable[[Any], bool]] = []

    for field_spec, value in filter_kwargs.items():
        field_path, lookup_type = parse_lookup(field_spec)
        op_func = LOOKUP_OPERATORS.get(lookup_type, LOOKUP_OPERATORS["exact"])

        # Capture loop variables via default args (avoids late-binding bug)
        def make_pred(fp=field_path, op=op_func, val=value):
            def predicate(record: Any) -> bool:
                record_value, found = resolve_field_value(record, fp)
                if not found and lookup_type != "isnull":
                    return False
                try:
                    return op(
                        record_value,
                        _coerce_temporal(record_value, lookup_type, val),
                    )
                except (TypeError, ValueError):
                    return False

            return predicate

        predicates.append(make_pred())

    def combined(record: Any) -> bool:
        return all(p(record) for p in predicates)

    return combined


def create_q_predicate(
    q_objects: List["Q"], default_connector: str = "AND"
) -> Callable[[Any], bool]:
    """Create a predicate from Q objects. Supports nested fields consistently."""
    from volnux.result.stream import Q

    def evaluate(q: "Q", record: Any) -> bool:
        result = _eval_children(q, record)
        return not result if q.negated else result

    def _eval_children(q: "Q", record: Any) -> bool:
        if not q.children:
            return True

        eval_fn = all if q.connector == "AND" else any
        return eval_fn(_eval_child(child, record) for child in q.children)

    def _eval_child(child: Any, record: Any) -> bool:
        if isinstance(child, Q):
            return evaluate(child, record)

        # (field_lookup, value) tuple — uses SAME resolve_field_value as filter kwargs
        field_spec, value = child
        field_path, lookup_type = parse_lookup(field_spec)
        op_func = LOOKUP_OPERATORS.get(lookup_type, LOOKUP_OPERATORS["exact"])

        record_value, found = resolve_field_value(record, field_path)
        if not found and lookup_type != "isnull":
            return False
        try:
            return op_func(
                record_value, _coerce_temporal(record_value, lookup_type, value)
            )
        except (TypeError, ValueError):
            return False

    root = (
        q_objects[0]
        if len(q_objects) == 1
        else Q(*q_objects, _connector=default_connector)
    )
    return lambda record: evaluate(root, record)
