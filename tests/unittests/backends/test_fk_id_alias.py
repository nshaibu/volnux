"""``<field>_object_id`` filters work on records held in memory, as they do on SQL
columns (OnDelete handling filters on that name)."""

from volnux.backends.q_compiler import create_filter_predicate, resolve_field_value


class Ref:
    def __init__(self, object_id):
        self.object_id = object_id


class Rec:
    def __init__(self, owner):
        self.owner = owner


def test_alias_on_an_object_with_a_reference():
    assert create_filter_predicate(owner_object_id="7")(Rec(Ref("7")))
    assert not create_filter_predicate(owner_object_id="8")(Rec(Ref("7")))


def test_alias_on_a_serialised_record():
    rec = {"owner": {"type": "foreignkey", "object_id": "7"}}
    assert create_filter_predicate(owner_object_id="7")(rec)
    assert not create_filter_predicate(owner_object_id="9")(rec)


def test_the_double_underscore_form_still_works():
    assert create_filter_predicate(owner__object_id="7")(Rec(Ref("7")))


def test_a_null_reference_and_unknown_names():
    assert resolve_field_value(Rec(None), "owner_object_id") == (None, True)
    assert resolve_field_value(Rec(Ref("7")), "other_object_id")[1] is False
    assert resolve_field_value(Rec(Ref("7")), "_object_id")[1] is False
