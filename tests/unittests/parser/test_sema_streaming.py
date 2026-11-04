"""Semantic rules for the streaming operator ``|->``.

A stream edge puts producer and consumer in one concurrent segment, so nothing that
needs the producer's final outcome (a branch) or a second attempt (retry), or that
merges streams (fan-in), may sit inside it.
"""

import pytest

from volnux.parser.grammar import pointy_parser
from volnux.parser.sema import Sema


def analyse(source):
    return Sema().analyse(pointy_parser(source))


def errors(source):
    return [d.message for d in analyse(source).errors]


ALLOWED = [
    "A |-> B",
    "A |-> B |-> C",
    "A -> B |-> C -> D",
    "A |-> (B || C)",  # fan-out
    "A |-> B (failure -> X, success -> Y)",  # branches on the tail of the stream
    "A |-> B |-> C (failure -> X, success -> Y)",
    "A (failure -> X, success -> Y) -> B",  # ordinary branches, no streaming
    "A (failure -> X, success -> Y)",
    "{ A |-> B } * 3",  # retry the whole stream
    "A -> B (failure -> X, success -> Y) -> C",
    "A || B -> C",
    "(A || B) |-> C",  # fan-in: C reads one merged stream
    "{A || B} |-> C",
    "X -> (A || B) |-> C",
    "A || B || C |-> D",
    "(A || B) |-> C |-> D",
    "(A || B) |-> C (failure -> X, success -> Y)",
]


@pytest.mark.parametrize("source", ALLOWED)
def test_allowed(source):
    assert errors(source) == []


class TestBranchesCannotStream:
    @pytest.mark.parametrize(
        "source",
        [
            "A (1 |-> C)",
            "A (success |-> C)",
            "A (0 |-> B, 1 |-> C)",
            "A (failure -> B, success |-> C)",
            "X -> A (failure |-> B)",
        ],
    )
    def test_a_streaming_branch_is_an_error(self, source):
        found = errors(source)
        assert any("branch cannot stream" in m for m in found), found

    def test_message_names_the_task_and_the_remedy(self):
        message = errors("Validate (success |-> Save)")[0]
        assert "Validate" in message and "only known when its stream ends" in message
        assert "Use '->'" in message

    def test_each_streaming_branch_is_reported(self):
        found = [m for m in errors("A (0 |-> B, 1 |-> C)") if "cannot stream" in m]
        assert len(found) == 2

    def test_plain_branches_are_untouched(self):
        assert errors("A (0 -> B, 1 -> C)") == []

    def test_custom_descriptors_follow_the_same_rule(self):
        source = "@descriptor: needs_review\nA (needs_review |-> B)"
        assert any("cannot stream" in m for m in errors(source))


class TestBranchesInsideAStream:
    @pytest.mark.parametrize(
        "source",
        [
            "A (failure -> X, success -> Y) |-> B",
            "A |-> B (failure -> X) |-> C",
            "X -> A (0 -> Y) |-> B",
        ],
    )
    def test_a_branched_task_followed_by_a_stream_edge_is_an_error(self, source):
        found = errors(source)
        assert any("has branches but is followed by '|->'" in m for m in found), found

    def test_branches_on_the_last_task_are_fine(self):
        assert errors("A |-> B |-> C (failure -> X, success -> Y)") == []

    def test_a_branch_before_a_non_stream_edge_is_fine(self):
        assert errors("A |-> B (failure -> X, success -> Y) -> C") == []


class TestFanIn:
    @pytest.mark.parametrize(
        "source",
        [
            "(A || B) |-> C",
            "{A || B} |-> C",
            "A || B || C |-> D",
            "X -> (A || B) |-> C",
        ],
    )
    def test_several_producers_may_feed_one_consumer(self, source):
        assert errors(source) == []

    def test_fan_out_is_allowed(self):
        assert errors("A |-> (B || C || D)") == []

    def test_fan_in_and_fan_out_together(self):
        assert errors("(A || B) |-> C |-> (D || E)") == []

    def test_a_retried_producer_in_a_fan_in_is_still_refused(self):
        assert any("retried" in m for m in errors("(A * 2 || B) |-> C"))

    def test_a_branched_task_before_a_fan_in_edge_is_still_refused(self):
        assert any("has branches" in m for m in errors("(A || B (0 -> X)) |-> C"))


class TestRetry:
    @pytest.mark.parametrize(
        "source",
        ["A * 3 |-> B", "A |-> B * 2", "A * 3 |-> B * 2", "A |-> (B * 2 || C)"],
    )
    def test_retrying_one_member_is_an_error(self, source):
        found = errors(source)
        assert any("cannot be retried on its own" in m for m in found), found

    def test_retrying_the_whole_stream_is_allowed(self):
        assert errors("{ A |-> B |-> C } * 3") == []

    def test_retry_without_streaming_is_untouched(self):
        assert errors("A * 3 -> B * 2") == []


class TestResultShape:
    def test_errors_make_the_result_failed_and_carry_a_location(self):
        result = analyse("A (1 |-> C)")
        assert result.errors and result.has_errors
        assert result.errors[0].message

    def test_clean_streams_have_no_warnings_from_these_rules(self):
        result = analyse("A |-> B |-> C")
        assert [w for w in result.warnings if "stream" in w.message.lower()] == []
