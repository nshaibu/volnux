import os
import typing

from .field import InputDataField
from .file_proxy import FileProxy
from volnux import default_batch_processors as batch_defaults
from volnux.constants import EMPTY

if typing.TYPE_CHECKING:
    from volnux.execution.pipeline import Pipeline

__all__ = ["FileInputDataField"]

# Map mode characters to os.access flags for permission validation
_MODE_ACCESS_FLAGS: dict[str, int] = {
    "r": os.R_OK,
    "w": os.W_OK,
    "a": os.W_OK,
    "x": os.W_OK,
    "+": os.R_OK | os.W_OK,
}


def _required_access_flags(mode: str) -> int:
    """
    Compute the os.access flags required by a file mode string.

    Args:
        mode: A mode string like "r", "rb", "w+", "a", etc.

    Returns:
        Bitwise OR of the required os.access flags.
    """
    flags = 0
    for char, flag in _MODE_ACCESS_FLAGS.items():
        if char in mode:
            flags |= flag
    # If no flags were matched (shouldn't happen), default to read
    return flags or os.R_OK


class FileInputDataField(InputDataField):
    """
    Pipeline descriptor for file path inputs.

    Stores the file path as the field value (str or os.PathLike). Returns a
    FileProxy when accessed. The FileProxy opens the file lazily on first I/O.

    Class definition example
    ------------------------
    class TradePipeline(Pipeline):
        class Meta:
            file = "workflows/trade.pty"

        # Required file — no default path
        input_file: FileInputDataField = FileInputDataField(required=True)

        # Optional file with a default path
        config_file: FileInputDataField = FileInputDataField(
            path="/etc/volnux/default.json",
            mode="r",
            encoding="utf-8",
        )

    Usage
    -----
    pipeline = TradePipeline(input_file="/data/trades_2024.csv")

    # Returns a FileProxy — use as context manager for automatic close
    with pipeline.input_file as f:
        records = list(csv.DictReader(f))

    # Or read directly (caller responsible for close)
    f = pipeline.input_file
    content = f.read()
    f.close()
    """

    def __init__(
        self,
        path: typing.Union[str, os.PathLike, None] = None,
        required: bool = False,
        chunk_size: int = batch_defaults.DEFAULT_CHUNK_SIZE,
        mode: str = "r",
        encoding: typing.Optional[str] = None,
        buffering: int = -1,
        errors: typing.Optional[str] = None,
        newline: typing.Optional[str] = None,
    ):
        """
        Initialise a FileInputDataField.

        Args:
            path: Default file path. Used when no path is assigned on the
                        pipeline instance. None means no default (field must be
                        set explicitly when required=True).
            required:   If True, the field must be set before the pipeline runs.
            chunk_size: Number of lines per batch chunk for streaming pipelines.
            mode:       File open mode ("r", "rb", "w", "w+", etc.).
            encoding:   Text encoding for text mode (ignored for binary mode).
            buffering:  Buffering policy passed to open().
            errors:     Error handling for encoding/decoding in text mode.
            newline:    Newline handling in text mode.

        Note on ``path`` vs ``name``
        ----------------------------
        ``path`` is the default *value* for the field — a filesystem path.
        It is passed as ``default=path`` to the parent InputDataField.

        The field's attribute *name* (e.g. "input_file") is set automatically
        by Python's descriptor protocol via ``__set_name__``. Never pass the
        attribute name as ``path``.
        """
        # Store proxy construction parameters — used in __get__
        self._file_mode = mode
        self._file_encoding = encoding
        self._file_buffering = buffering
        self._file_errors = errors
        self._file_newline = newline

        super().__init__(
            # name is None — set by __set_name__ to the attribute name
            name=None,
            required=required,
            data_type=(str, os.PathLike),
            # path is the DEFAULT VALUE, not the field name
            default=path if path is not None else EMPTY,
            batch_size=chunk_size,
            batch_processor=batch_defaults.file_stream_batch_processor,
        )

    def __set__(self, instance: "Pipeline", value: typing.Any) -> None:
        """
        Set and validate the file path.

        Raises:
            TypeError:  If value is not str or PathLike.
            ValueError: If the path does not exist, is not a file, or is not
                        accessible with the configured mode.
        """
        if value is not None:
            if not os.path.isfile(value):
                raise ValueError(
                    f"FileInputDataField {self.name!r}: "
                    f"{value!r} is not a regular file or does not exist."
                )

            required_flags = _required_access_flags(self._file_mode)
            if not os.access(value, required_flags):
                mode_desc = f"mode={self._file_mode!r}"
                raise ValueError(
                    f"FileInputDataField {self.name!r}: "
                    f"{value!r} is not accessible with {mode_desc}. "
                    "Check file permissions."
                )

        super().__set__(instance, value)

    def __get__(
        self,
        instance: typing.Optional["Pipeline"],
        owner: typing.Optional[object] = None,
    ) -> typing.Optional[FileProxy]:
        """
        Return a FileProxy for the stored file path.

        A new FileProxy is created on each access. The file is opened
        lazily on first I/O — construction is inexpensive.

        Why a new FileProxy each time (not cached):
            FileProxy holds an open file handle, which cannot be pickled.
            Caching it in instance.__dict__ would break ProcessPoolExecutor
            dispatch in BatchPipeline. Callers must store the reference and
            close it explicitly, or use it as a context manager.

        Returns:
            FileProxy for the field's path, or None if no path is set.
        """
        if instance is None:
            return self  # type: ignore[return-value]

        path: typing.Union[str, os.PathLike, None] = super().__get__(instance, owner)

        if not path:
            return None

        kwargs: dict = {
            "mode": self._file_mode,
            "buffering": self._file_buffering,
        }
        if "b" not in self._file_mode:
            # Text-mode-only parameters — add only when relevant
            if self._file_encoding is not None:
                kwargs["encoding"] = self._file_encoding
            if self._file_errors is not None:
                kwargs["errors"] = self._file_errors
            if self._file_newline is not None:
                kwargs["newline"] = self._file_newline

        return FileProxy(file_path=path, **kwargs)

    def __repr__(self) -> str:
        return (
            f"<FileInputDataField"
            f" name={self.name!r}"
            f" required={self.required}"
            f" mode={self._file_mode!r}"
            f" default={self.default!r}>"
        )
