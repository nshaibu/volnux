import io
import os
import typing
from unittest.mock import MagicMock, mock_open, patch

import pytest

from volnux import default_batch_processors as batch_defaults
from volnux.constants import EMPTY, UNKNOWN
from volnux.exceptions import ImproperlyConfigured
from volnux.fields import FileInputDataField, InputDataField
from volnux.fields.file_proxy import FileProxy


class TestInputDataField:
    def test_init_basic(self):
        field = InputDataField(name="test")
        assert field.name == "test"
        assert field.required is False
        assert field.default is EMPTY
        assert field.data_type == (UNKNOWN,)

    def test_init_with_required(self):
        field = InputDataField(name="test", required=True)
        assert field.required is True

    def test_init_with_data_type(self):
        field = InputDataField(data_type=str)
        assert field.data_type == (str,)

        field = InputDataField(data_type=(str, int))
        assert field.data_type == (str, int)

    def test_data_type_validation(self):
        field = InputDataField(name="test", data_type=str)
        mock_instance = MagicMock()

        with pytest.raises(TypeError):
            field.__set__(mock_instance, 123)

    def test_required_field_validation(self):
        field = InputDataField(name="test", required=True)
        mock_instance = MagicMock()

        with pytest.raises(ValueError):
            field.__set__(mock_instance, None)

    def test_default_value(self):
        field = InputDataField(name="test", default="default_value")
        mock_instance = MagicMock()
        mock_instance.__dict__ = {}

        assert field.__get__(mock_instance) == "default_value"

    @pytest.mark.parametrize("data_type", [list, tuple, typing.List[str]])
    def test_batch_processor_for_sequences(self, data_type):
        field = InputDataField(name="test", data_type=data_type)
        assert field.batch_processor is batch_defaults.list_batch_processor
        assert field.has_batch_operation is True

    @pytest.mark.parametrize("data_type", [str, dict, set])
    def test_no_batch_processor_for_non_sequences(self, data_type):
        field = InputDataField(name="test", data_type=data_type)
        assert field.batch_processor is None

    @pytest.mark.parametrize(
        "data_type, value",
        [(list, [1, "a"]), (typing.List[str], ["a", "b"]), (tuple, (1,))],
    )
    def test_collection_field_accepts_collection_value(self, data_type, value):
        field = InputDataField(name="test", data_type=data_type)
        mock_instance = MagicMock()
        mock_instance.__dict__ = {}

        field.__set__(mock_instance, value)
        assert mock_instance.__dict__["test"] == value

    @pytest.mark.parametrize(
        "data_type, value, match",
        [
            (typing.List[str], "a", "expects"),
            (typing.List[str], ["a", 1], "element at index 1"),
        ],
    )
    def test_collection_field_rejects_invalid_value(self, data_type, value, match):
        field = InputDataField(name="test", data_type=data_type)

        with pytest.raises(TypeError, match=match):
            field.__set__(MagicMock(), value)

    def test_explicit_batch_mode_expects_collection_of_data_type(self):
        field = InputDataField(name="test", data_type=str).enable_batch_mode()
        mock_instance = MagicMock()
        mock_instance.__dict__ = {}

        field.__set__(mock_instance, ["a", "b"])
        with pytest.raises(TypeError, match="batch mode"):
            field.__set__(mock_instance, "a")
        with pytest.raises(TypeError, match="element at index 1"):
            field.__set__(mock_instance, ["a", 1])

    def test_invalid_batch_processor(self):
        with pytest.raises(ValueError):
            field = InputDataField(name="test", batch_processor=123)

    def test_set_name_descriptor(self):
        field = InputDataField()
        field.__set_name__(None, "dynamic_name")
        assert field.name == "dynamic_name"

    def test_cache_operations(self):
        field = InputDataField(name="test", data_type=str)
        mock_instance = MagicMock()
        field.__set__(mock_instance, "value")

        mock_instance.get_pipeline_state().set_cache_for_pipeline_field.assert_called_once_with(
            mock_instance, "test", "value"
        )

    def test_default_factory(self):
        factory = lambda: "from_factory"
        field = InputDataField(name="test", default_factory=factory)

        mock_instance = MagicMock()
        mock_instance.__dict__ = {}

        assert field.__get__(mock_instance) == "from_factory"


class TestFileInputDataField:
    def test_init(self):
        field = FileInputDataField(path="test.txt")
        # ``path`` is the default value; the name comes from __set_name__
        assert field.name is None
        assert field.default == "test.txt"
        assert field.data_type == (str, os.PathLike)
        assert field.batch_processor is batch_defaults.file_stream_batch_processor

    def test_set_name_uses_attribute_name(self):
        class Holder:
            input_file = FileInputDataField(path="test.txt")

        assert Holder.input_file.name == "input_file"

    @patch("os.access", return_value=True)
    @patch("os.path.isfile", return_value=True)
    def test_valid_file(self, mock_isfile, mock_access):
        field = FileInputDataField()
        mock_instance = MagicMock()

        field.__set__(mock_instance, "existing_file.txt")
        mock_isfile.assert_called_once_with("existing_file.txt")
        mock_access.assert_called_once_with("existing_file.txt", os.R_OK)

    @pytest.mark.parametrize(
        "mode, flags",
        [("r", os.R_OK), ("w", os.W_OK), ("a", os.W_OK), ("r+", os.R_OK | os.W_OK)],
    )
    @patch("os.access", return_value=True)
    @patch("os.path.isfile", return_value=True)
    def test_access_check_matches_mode(self, mock_isfile, mock_access, mode, flags):
        field = FileInputDataField(mode=mode)

        field.__set__(MagicMock(), "file.txt")
        mock_access.assert_called_once_with("file.txt", flags)

    @patch("os.access", return_value=False)
    @patch("os.path.isfile", return_value=True)
    def test_inaccessible_file(self, mock_isfile, mock_access):
        field = FileInputDataField(mode="w")

        with pytest.raises(ValueError, match="not accessible with mode='w'"):
            field.__set__(MagicMock(), "readonly.txt")

    @patch("os.path.isfile")
    def test_invalid_file(self, mock_isfile):
        mock_isfile.return_value = False
        field = FileInputDataField()
        mock_instance = MagicMock()

        with pytest.raises(ValueError):
            field.__set__(mock_instance, "non_existing_file.txt")

    @patch("os.access", return_value=True)
    @patch("os.path.isfile", return_value=True)
    def test_get_returns_lazy_file_proxy(self, mock_isfile, mock_access):
        field = FileInputDataField(encoding="utf-8")
        field.__set_name__(None, "input_file")
        mock_instance = MagicMock()
        mock_instance.__dict__ = {}
        field.__set__(mock_instance, "test.txt")

        with patch("builtins.open") as mock_builtin_open:
            result = field.__get__(mock_instance, None)
            mock_builtin_open.assert_not_called()

        assert isinstance(result, FileProxy)
        assert result.name == "test.txt"
        assert result.mode == "r"
        assert result.encoding == "utf-8"
        # A fresh proxy per access, so handles are never shared or pickled
        assert field.__get__(mock_instance, None) is not result

    def test_get_returns_none_without_path(self):
        field = FileInputDataField()
        field.__set_name__(None, "input_file")
        mock_instance = MagicMock()
        mock_instance.__dict__ = {}

        assert field.__get__(mock_instance, None) is None

    def test_required_file(self):
        field = FileInputDataField(required=True)
        mock_instance = MagicMock()

        with pytest.raises(ValueError):
            field.__set__(mock_instance, None)


class TestFileProxy:
    def test_initialization(self):
        proxy = FileProxy("test.txt", mode="r", encoding="utf-8")
        assert proxy.file_path == "test.txt"
        assert proxy.mode == "r"
        assert proxy.encoding == "utf-8"
        assert proxy.closed is False

    def test_repr(self):
        proxy = FileProxy("test.txt")
        assert repr(proxy) == "<FileProxy 'test.txt' mode='r' [unopened]>"

    @patch("builtins.open", new_callable=mock_open, read_data="data")
    def test_ensure_open(self, mock_file):
        proxy = FileProxy("test.txt", mode="r")
        file = proxy._ensure_open()
        # opener is only forwarded when set
        mock_file.assert_called_once_with(
            "test.txt",
            mode="r",
            buffering=-1,
            closefd=True,
            encoding=None,
            errors=None,
            newline=None,
        )
        assert file.read() == "data"

    @patch("builtins.open", new_callable=mock_open)
    def test_ensure_open_binary_mode_omits_text_options(self, mock_file):
        opener = MagicMock()
        FileProxy("test.bin", mode="rb", encoding="utf-8", opener=opener)._ensure_open()

        mock_file.assert_called_once_with(
            "test.bin", mode="rb", buffering=-1, closefd=True, opener=opener
        )

    @patch("builtins.open", new_callable=mock_open)
    def test_ensure_open_reuses_handle(self, mock_file):
        proxy = FileProxy("test.txt")
        assert proxy._ensure_open() is proxy._ensure_open()
        mock_file.assert_called_once()

    @patch("builtins.open", new_callable=mock_open)
    def test_closed_proxy_cannot_reopen(self, mock_file):
        proxy = FileProxy("test.txt")
        proxy.close()

        with pytest.raises(ValueError, match="closed FileProxy"):
            proxy.read()
        mock_file.assert_not_called()

    def test_registered_as_io_base(self):
        assert isinstance(FileProxy("test.txt"), io.IOBase)

    @patch("builtins.open", new_callable=mock_open)
    def test_read(self, mock_file):
        proxy = FileProxy("test.txt", mode="r")
        proxy.read()
        mock_file.assert_called_once()

    @patch("builtins.open", new_callable=mock_open)
    def test_write(self, mock_file):
        proxy = FileProxy("test.txt", mode="w")
        proxy.write("test data")
        mock_file.assert_called_once()
        mock_file().write.assert_called_once_with("test data")

    @patch("builtins.open", new_callable=mock_open)
    def test_close(self, mock_file):
        proxy = FileProxy("test.txt", mode="r")
        proxy._ensure_open()
        proxy._closed = False
        mock_file().closed = False
        proxy.close()
        mock_file().close.assert_called_once()
        assert proxy.closed is True

    @patch("builtins.open", new_callable=mock_open)
    def test_context_manager(self, mock_file):
        mock_file().closed = False
        with FileProxy("test.txt", mode="r") as proxy:
            proxy._closed = False
            proxy.read()
        mock_file.assert_called()
        mock_file().close.assert_called_once()

    @patch("builtins.open", new_callable=mock_open)
    def test_readline(self, mock_file):
        mock_file.return_value.readline.return_value = "line1\n"
        proxy = FileProxy("test.txt", mode="r")
        result = proxy.readline()
        assert result == "line1\n"

    @patch("builtins.open", new_callable=mock_open)
    def test_readlines(self, mock_file):
        mock_file.return_value.readlines.return_value = ["line1\n", "line2\n"]
        proxy = FileProxy("test.txt", mode="r")
        result = proxy.readlines()
        assert result == ["line1\n", "line2\n"]

    @patch("builtins.open", new_callable=mock_open)
    def test_seek_and_tell(self, mock_file):
        proxy = FileProxy("test.txt", mode="r")
        proxy._ensure_open()
        proxy.seek(10)
        mock_file().seek.assert_called_once_with(10, 0)
        proxy.tell()
        mock_file().tell.assert_called_once()

    @patch("builtins.open", new_callable=mock_open)
    def test_truncate(self, mock_file):
        proxy = FileProxy("test.txt", mode="w")
        proxy._ensure_open()
        proxy.truncate(100)
        mock_file().truncate.assert_called_once_with(100)

    @patch("builtins.open", new_callable=mock_open, read_data="line1\nline2\n")
    def test_iter(self, mock_file):
        proxy = FileProxy("test.txt", mode="r")
        lines = list(proxy)
        assert lines == ["line1\n", "line2\n"]
