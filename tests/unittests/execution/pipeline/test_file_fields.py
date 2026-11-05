"""FileInputDataField, FileChunk and FileProxy, on real files."""

import io
import os
import pickle
import stat
import sys

import pytest

from volnux.exceptions import ImproperlyConfigured
from volnux.execution.pipeline.fields import (
    FileChunk,
    FileInputDataField,
    InputDataField,
)
from volnux.execution.pipeline.fields.file import _required_access_flags
from volnux.execution.pipeline.fields.file_proxy import FileProxy


@pytest.fixture
def text_file(tmp_path):
    path = tmp_path / "data.txt"
    path.write_text("line one\nline two\nline three\n", encoding="utf-8")
    return path


# =============================== FileProxy ===================================


def test_the_file_is_opened_on_first_use_not_at_construction(text_file):
    proxy = FileProxy(text_file)
    assert proxy._file is None and not proxy.closed
    assert "unopened" in repr(proxy)

    proxy.read(1)

    assert proxy._file is not None and "open" in repr(proxy)
    proxy.close()


def test_reading_all_or_some(text_file):
    with FileProxy(text_file) as f:
        assert f.read(4) == "line"
        assert f.read() == " one\nline two\nline three\n"
        assert f.read() == ""


def test_lines_and_iteration(text_file):
    with FileProxy(text_file) as f:
        assert f.readline() == "line one\n"
        assert f.readlines() == ["line two\n", "line three\n"]
    with FileProxy(text_file) as f:
        assert [line.strip() for line in f] == ["line one", "line two", "line three"]
        assert iter(f) is f


def test_a_proxy_can_feed_the_csv_module(tmp_path):
    import csv

    path = tmp_path / "t.csv"
    path.write_text("a,b\n1,2\n3,4\n")
    with FileProxy(path) as f:
        rows = list(csv.DictReader(f))
    assert rows == [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]


def test_writing_and_reading_back(tmp_path):
    path = tmp_path / "out.txt"
    with FileProxy(path, mode="w") as f:
        assert f.write("abc") == 3
        f.writelines(["d\n", "e\n"])
        f.flush()
    assert path.read_text() == "abcd\ne\n"


def test_seek_tell_truncate(text_file):
    with FileProxy(text_file, mode="r+") as f:
        assert f.seek(5) == 5 and f.tell() == 5
        assert f.read(3) == "one"
        f.seek(0)
        f.truncate(4)
    assert text_file.read_text() == "line"


def test_binary_mode_ignores_text_options(tmp_path):
    path = tmp_path / "b.bin"
    path.write_bytes(b"\x00\x01\x02")
    with FileProxy(
        path, mode="rb", encoding="utf-8", errors="strict", newline="\n"
    ) as f:
        assert f.read() == b"\x00\x01\x02"


def test_text_options_are_applied(tmp_path):
    path = tmp_path / "e.txt"
    path.write_bytes("é\r\n".encode("latin-1"))
    with FileProxy(path, encoding="latin-1", newline="") as f:
        assert f.read() == "é\r\n"


def test_a_closed_proxy_is_closed_for_good(text_file):
    f = FileProxy(text_file)
    f.read(1)
    f.close()
    f.close()  # twice is fine

    assert f.closed and "closed" in repr(f)
    with pytest.raises(ValueError, match="closed FileProxy"):
        f.read()
    with pytest.raises(ValueError):
        f.readline()


def test_closing_an_unopened_proxy_prevents_a_later_open(text_file):
    f = FileProxy(text_file)
    f.close()
    with pytest.raises(ValueError):
        f.read()


def test_the_context_manager_closes_even_on_an_error(text_file):
    with pytest.raises(RuntimeError):
        with FileProxy(text_file) as f:
            f.read(1)
            raise RuntimeError("body failed")
    assert f.closed


def test_the_proxy_reports_when_the_real_file_was_closed_behind_it(text_file):
    f = FileProxy(text_file)
    f.read(1)
    f._file.close()
    assert f.closed


def test_a_missing_file_fails_on_first_io_not_at_construction(tmp_path):
    f = FileProxy(tmp_path / "absent.txt")
    with pytest.raises(FileNotFoundError):
        f.read()


def test_flush_before_opening_is_a_no_op(text_file):
    FileProxy(text_file).flush()


@pytest.mark.parametrize(
    "mode, readable, writable",
    [
        ("r", True, False),
        ("rb", True, False),
        ("w", False, True),
        ("a", False, True),
        ("x", False, True),
        ("xb", False, True),
        ("r+", True, True),
        ("w+", True, True),
    ],
)
def test_readable_and_writable_follow_the_mode(mode, readable, writable):
    """'x' (exclusive creation) writes, and used to be reported as not writable."""
    f = FileProxy("unused", mode=mode)
    assert (f.readable(), f.writable()) == (readable, writable)
    assert f.mode == mode


def test_seekable_is_speculative_until_open_and_false_once_closed(text_file):
    f = FileProxy(text_file)
    assert f.seekable() is True  # not opened yet: assumed
    f.read(1)
    assert f.seekable() is True
    f.close()
    assert f.seekable() is False  # it used to claim to be seekable after closing


def test_name_fileno_and_isatty(text_file):
    with FileProxy(text_file) as f:
        assert f.name == str(text_file)
        assert isinstance(f.fileno(), int)
        assert f.isatty() is False


def test_the_proxy_is_registered_as_an_io_base():
    assert isinstance(FileProxy("x"), io.IOBase)


def test_garbage_collection_closes_the_file(text_file):
    f = FileProxy(text_file)
    f.read(1)
    raw = f._file
    del f
    import gc

    gc.collect()
    assert raw.closed


def test_a_failing_close_in_the_destructor_is_swallowed():
    f = FileProxy("x")
    f.close = lambda: (_ for _ in ()).throw(RuntimeError("boom"))
    f.__del__()  # must not raise


# ============================== FileChunk ====================================


def test_a_chunk_is_a_frozen_picklable_value():
    chunk = FileChunk(data="abc", source="/x", index=2)

    assert pickle.loads(pickle.dumps(chunk)) == chunk
    with pytest.raises(Exception):
        chunk.data = "other"
    assert FileChunk(b"x", "s", 0).data == b"x"


# ========================= FileInputDataField ================================


def test_access_flags_follow_the_mode():
    assert _required_access_flags("r") == os.R_OK
    assert _required_access_flags("rb") == os.R_OK
    assert _required_access_flags("w") == os.W_OK
    assert _required_access_flags("a") == os.W_OK
    assert _required_access_flags("r+") == os.R_OK | os.W_OK
    assert _required_access_flags("") == os.R_OK  # unknown: read


@pytest.fixture
def file_pipeline(pipeline_class, event_class):
    def build(**kwargs):
        field = FileInputDataField(**kwargs)
        return pipeline_class(event_class().__name__, book=field), field

    return build


def test_a_path_default_is_the_default_value_not_the_name():
    f = FileInputDataField(path="/etc/default.json", mode="rb", chunk_size=64)
    assert f.default == "/etc/default.json" and f.name is None
    assert f.batch_size == 64 and f.has_batch_operation


def test_no_path_means_no_default():
    from volnux.constants import EMPTY

    assert FileInputDataField().default is EMPTY


def test_reading_the_field_gives_a_lazy_proxy_each_time(file_pipeline, text_file):
    cls, _ = file_pipeline()
    pipeline = cls(book=str(text_file))

    first, second = pipeline.book, pipeline.book

    assert isinstance(first, FileProxy) and first is not second
    assert first._file is None  # nothing opened by reading the attribute
    assert first.read() == text_file.read_text()
    first.close()
    assert second.read(4) == "line"  # a separate handle
    second.close()


def test_the_proxy_gets_the_fields_mode_and_text_options(file_pipeline, text_file):
    cls, _ = file_pipeline(
        mode="r", encoding="utf-8", errors="strict", newline="\n", buffering=1
    )

    proxy = cls(book=text_file).book

    assert (
        proxy.mode,
        proxy.encoding,
        proxy.errors,
        proxy.newline,
        proxy.buffering,
    ) == (
        "r",
        "utf-8",
        "strict",
        "\n",
        1,
    )
    proxy.close()


def test_binary_fields_get_no_text_options(file_pipeline, tmp_path):
    path = tmp_path / "b.bin"
    path.write_bytes(b"abc")
    cls, _ = file_pipeline(mode="rb", encoding="utf-8")

    proxy = cls(book=path).book

    assert proxy.encoding is None and proxy.read() == b"abc"
    proxy.close()


def test_an_unset_field_reads_none(file_pipeline):
    cls, _ = file_pipeline()
    assert cls().book is None


def test_the_default_path_is_used_when_none_is_given(file_pipeline, text_file):
    cls, _ = file_pipeline(path=str(text_file))
    proxy = cls().book
    assert proxy.read(4) == "line"
    proxy.close()


def test_a_path_object_is_accepted(file_pipeline, text_file):
    cls, _ = file_pipeline()
    assert cls(book=text_file).book.file_path == text_file


def test_a_missing_path_or_a_directory_is_rejected(file_pipeline, tmp_path):
    cls, _ = file_pipeline()
    with pytest.raises(ValueError, match="not a regular file or does not exist"):
        cls(book=str(tmp_path / "absent.txt"))
    with pytest.raises(ValueError, match="not a regular file"):
        cls(book=str(tmp_path))


@pytest.mark.parametrize("bad", [12345, 1.5, ["a"], {"a": 1}])
def test_a_value_that_is_not_a_path_is_a_type_error(file_pipeline, bad):
    """An int used to be taken for a file descriptor by os.path.isfile."""
    cls, _ = file_pipeline()
    with pytest.raises(TypeError, match="str or os.PathLike"):
        cls(book=bad)


@pytest.mark.skipif(
    sys.platform == "win32" or os.geteuid() == 0,
    reason="needs POSIX permissions, not root",
)
def test_a_file_the_mode_cannot_use_is_rejected(file_pipeline, tmp_path):
    path = tmp_path / "locked.txt"
    path.write_text("x")
    path.chmod(0)
    try:
        cls, _ = file_pipeline(mode="r")
        with pytest.raises(ValueError, match="not accessible with mode='r'"):
            cls(book=str(path))
    finally:
        path.chmod(stat.S_IRUSR | stat.S_IWUSR)


def test_a_write_mode_needs_an_existing_writable_file(file_pipeline, text_file):
    cls, _ = file_pipeline(mode="w")
    assert cls(book=text_file).book.writable()


def test_a_required_file_needs_a_value(file_pipeline):
    cls, _ = file_pipeline(required=True)
    cls.construct_call_signature()
    with pytest.raises(TypeError):
        cls()


def test_none_clears_an_optional_file(file_pipeline, text_file):
    cls, _ = file_pipeline()
    pipeline = cls(book=text_file)
    pipeline.book = None
    assert pipeline.book is None


def test_a_file_chunk_is_stored_and_read_back_as_the_chunk(file_pipeline):
    cls, _ = file_pipeline()
    chunk = FileChunk(data="piece", source="/x", index=0)

    pipeline = cls(book=chunk)

    assert pipeline.book is chunk  # the data itself, not a proxy


def test_a_chunk_survives_pickling_inside_a_pipeline_dict(file_pipeline):
    cls, _ = file_pipeline()
    pipeline = cls(book=FileChunk("p", "/x", 3))

    state = pickle.loads(pickle.dumps(pipeline.__getstate__()))

    assert state["book"] == FileChunk("p", "/x", 3)


def test_chunks_of_a_text_file(file_pipeline, text_file):
    _, field = file_pipeline()

    chunks = list(field.iter_chunks(text_file, 10))

    assert "".join(c.data for c in chunks) == text_file.read_text()
    assert [c.index for c in chunks] == list(range(len(chunks)))
    assert all(len(c.data) <= 10 for c in chunks) and len(chunks) == 3
    assert {c.source for c in chunks} == {str(text_file)}


def test_chunks_of_a_binary_file_are_bytes(file_pipeline, tmp_path):
    path = tmp_path / "b.bin"
    path.write_bytes(bytes(range(25)))
    _, field = file_pipeline(mode="rb")

    chunks = list(field.iter_chunks(path, 10))

    assert [len(c.data) for c in chunks] == [10, 10, 5]
    assert b"".join(c.data for c in chunks) == bytes(range(25))


def test_an_empty_file_has_no_chunks(file_pipeline, tmp_path):
    path = tmp_path / "empty.txt"
    path.write_text("")
    _, field = file_pipeline()
    assert list(field.iter_chunks(path, 10)) == []


def test_the_file_is_open_only_while_chunks_are_consumed(
    file_pipeline, text_file, monkeypatch
):
    _, field = file_pipeline()
    opened = []
    real_open = field.open_stream

    def tracking(path):
        stream = real_open(path)
        opened.append(stream)
        return stream

    monkeypatch.setattr(field, "open_stream", tracking)

    chunks = field.iter_chunks(text_file, 5)
    assert opened == []  # a generator: nothing opened yet
    next(chunks)
    assert not opened[0].closed
    list(chunks)
    assert opened[0].closed

    again = field.iter_chunks(text_file, 5)
    next(again)
    again.close()  # dropped early: the file is closed too
    assert opened[1].closed


def test_open_stream_uses_the_fields_options(file_pipeline, text_file):
    _, field = file_pipeline(mode="r", encoding="utf-8")
    with field.open_stream(text_file) as stream:
        assert stream.encoding.lower().replace("-", "") == "utf8" and stream.mode == "r"


def test_repr_shows_the_mode_and_default():
    f = FileInputDataField(path="/p", mode="rb")
    f.name = "book"
    assert (
        "name='book'" in repr(f)
        and "mode='rb'" in repr(f)
        and "default='/p'" in repr(f)
    )
