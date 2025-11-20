import os
from unittest.mock import MagicMock, mock_open, patch

import pytest

from volnux.constants import EMPTY, UNKNOWN
from volnux.exceptions import ImproperlyConfigured
from volnux.fields import FileInputDataField, FileProxy, InputDataField


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

    def test_batch_processor_for_list(self):
        field = InputDataField(name="test", data_type=list)
        assert field.batch_processor is not None
        assert field.has_batch_operation is True

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
        assert field.name == "test.txt"
        assert field.data_type == (str, os.PathLike)
        assert field.batch_processor is not None

    @patch("os.path.isfile")
    def test_valid_file(self, mock_isfile):
        mock_isfile.return_value = True
        field = FileInputDataField()
        mock_instance = MagicMock()

        field.__set__(mock_instance, "existing_file.txt")
        mock_isfile.assert_called_once_with("existing_file.txt")

    @patch("os.path.isfile")
    def test_invalid_file(self, mock_isfile):
        mock_isfile.return_value = False
        field = FileInputDataField()
        mock_instance = MagicMock()

        with pytest.raises(ValueError):
            field.__set__(mock_instance, "non_existing_file.txt")

    @patch("volnux.fields.FileProxy")
    @patch("os.path.isfile")
    def test_file_opening(self, mock_isfile, mock_open):
        mock_isfile.return_value = True
        mock_file = MagicMock()
        mock_open.return_value = mock_file

        field = FileInputDataField()
        mock_instance = MagicMock()
        field.__set__(mock_instance, "test.txt")

        result = field.__get__(mock_instance, None)
        mock_open.assert_called()
        assert result == mock_file

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
        assert repr(proxy) == "<FileProxy test.txt (unopened)>"

    @patch("builtins.open", new_callable=mock_open, read_data="data")
    def test_ensure_open(self, mock_file):
        proxy = FileProxy("test.txt", mode="r")
        file = proxy._ensure_open()
        mock_file.assert_called_once_with(
            "test.txt",
            mode="r",
            buffering=-1,
            encoding=None,
            errors=None,
            newline=None,
            closefd=True,
            opener=None,
        )
        assert file.read() == "data"

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

    @patch("builtins.open", new_callable=mock_open)
    def test_open_for_operation(self, mock_file):
        proxy = FileProxy("test.txt", mode="r")
        result = proxy.open_for_operation(lambda f: f.read())
        mock_file.assert_called_once()
        assert result == mock_file().read()
