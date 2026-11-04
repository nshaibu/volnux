import io
import os
import typing
from types import TracebackType

__all__ = ["FileProxy"]


class FileProxy:
    """
    A lazy-opening proxy that implements the full file protocol.

    The underlying file is opened on the first I/O call. Subsequent calls
    reuse the same open file handle. Calling ``close()`` marks the proxy
    as closed permanently — it cannot be re-opened. Create a new FileProxy
    to open the same path again.

    Thread safety: FileProxy is NOT thread-safe. Do not share a single
    FileProxy instance across multiple threads.

    Usage
    -----
        # Lazy — file not opened yet
        proxy = FileProxy("/data/trades.csv", mode="r", encoding="utf-8")

        # File opened on first I/O operation
        content = proxy.read()

        # Context manager — explicit lifecycle
        with FileProxy("/data/out.csv", mode="w") as f:
            f.write("header\\n")
            f.write("data\\n")

        # As a drop-in for functions expecting a file-like object
        import csv
        with FileProxy("/data/trades.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                process(row)
    """

    def __init__(
        self,
        file_path: typing.Union[str, os.PathLike],
        mode: str = "r",
        buffering: int = -1,
        encoding: typing.Optional[str] = None,
        errors: typing.Optional[str] = None,
        newline: typing.Optional[str] = None,
        closefd: bool = True,
        opener: typing.Optional[typing.Callable[[str, int], int]] = None,
    ) -> None:
        self.file_path = file_path
        self._mode = mode
        self.buffering = buffering
        self.encoding = encoding
        self.errors = errors
        self.newline = newline
        self.closefd = closefd
        self.opener = opener

        self._file: typing.Optional[typing.IO] = None
        self._closed: bool = False

    def _ensure_open(self) -> typing.IO:
        """
        Return the open file handle, opening it lazily on first call.

        Raises:
            ValueError: If the proxy has been closed.
        """
        if self._closed:
            raise ValueError(f"I/O operation on closed FileProxy {self.file_path!r}")
        if self._file is None:
            kwargs: dict = {
                "mode": self._mode,
                "buffering": self.buffering,
                "closefd": self.closefd,
            }
            if self.opener is not None:
                kwargs["opener"] = self.opener
            if "b" not in self._mode:
                # Text-mode-only parameters
                kwargs["encoding"] = self.encoding
                kwargs["errors"] = self.errors
                kwargs["newline"] = self.newline

            self._file = open(self.file_path, **kwargs)

        return self._file

    @property
    def closed(self) -> bool:
        """True if the proxy is closed or the underlying file has been closed."""
        return self._closed or (self._file is not None and self._file.closed)

    def close(self) -> None:
        """Close the underlying file and mark the proxy as permanently closed."""
        if self._file is not None and not self._file.closed:
            self._file.close()
        self._closed = True
        self._file = None

    def __enter__(self) -> "FileProxy":
        return self

    def __exit__(
        self,
        exc_type: typing.Optional[typing.Type[BaseException]],
        exc_val: typing.Optional[BaseException],
        exc_tb: typing.Optional[TracebackType],
    ) -> None:
        self.close()

    def __del__(self) -> None:
        """
        Attempt to close the file on garbage collection.
        Exceptions are suppressed — GC runs in arbitrary context and
        exception propagation from __del__ causes ResourceWarning noise.
        """
        try:
            self.close()
        except Exception:
            pass

    def read(self, size: typing.Optional[int] = None) -> typing.Union[str, bytes]:
        """
        Read from the file.

        Args:
            size: Maximum number of bytes/characters to read.
                  None (default) reads the entire remaining file.

        Returns:
            The content read from the file.
        """
        f = self._ensure_open()
        if size is not None:
            return f.read(size)
        return f.read()

    def readline(self, size: int = -1) -> typing.Union[str, bytes]:
        """Read and return one line from the file."""
        return self._ensure_open().readline(size)

    def readlines(self, hint: int = -1) -> typing.List[typing.Union[str, bytes]]:
        """Read and return all lines as a list."""
        return self._ensure_open().readlines(hint)

    def write(self, s: typing.AnyStr) -> int:
        """Write string/bytes to the file. Returns number of characters written."""
        return self._ensure_open().write(s)

    def writelines(self, lines: typing.Iterable[typing.AnyStr]) -> None:
        """Write a list of lines to the file."""
        self._ensure_open().writelines(lines)

    def seek(self, offset: int, whence: int = 0) -> int:
        """
        Set the current file position.

        Args:
            offset: Byte offset relative to ``whence``.
            whence: 0 = beginning, 1 = current position, 2 = end of file.

        Returns:
            The new absolute file position.
        """
        return self._ensure_open().seek(offset, whence)

    def tell(self) -> int:
        """Return the current file position."""
        return self._ensure_open().tell()

    def truncate(self, size: typing.Optional[int] = None) -> int:
        """Truncate the file to at most ``size`` bytes. Returns new file size."""
        return self._ensure_open().truncate(size)

    def flush(self) -> None:
        """Flush the write buffer. No-op if the file is not yet open."""
        if self._file is not None:
            self._file.flush()

    def fileno(self) -> int:
        """Return the underlying file descriptor."""
        return self._ensure_open().fileno()

    def isatty(self) -> bool:
        """Return True if the file is connected to a TTY."""
        return self._ensure_open().isatty()

    @property
    def name(self) -> str:
        """The file path as a string."""
        return str(self.file_path)

    @property
    def mode(self) -> str:
        """The mode string passed at construction."""
        return self._mode

    def readable(self) -> bool:
        """True if the file was opened for reading."""
        return "r" in self._mode or "+" in self._mode

    def writable(self) -> bool:
        """True if the file was opened for writing."""
        return "w" in self._mode or "a" in self._mode or "+" in self._mode

    def seekable(self) -> bool:
        """
        True if the file supports random access.

        If the file has not been opened yet, returns True speculatively —
        most regular files are seekable. Pipes and sockets are not, but
        this cannot be determined without opening them.
        """
        if self._file is not None:
            return self._file.seekable()
        return True

    def __iter__(self) -> "FileProxy":
        """Return self as an iterator (consistent with file object behaviour)."""
        return self

    def __next__(self) -> typing.AnyStr:
        """Return the next line, or raise StopIteration at EOF."""
        line = self.readline()
        if not line:
            raise StopIteration
        return line  # type: ignore[return-value]

    def __repr__(self) -> str:
        status = "closed" if self.closed else ("open" if self._file else "unopened")
        return f"<FileProxy {self.file_path!r} mode={self._mode!r} [{status}]>"


# Register with io.IOBase so isinstance(proxy, io.IOBase) returns True.
# This allows FileProxy to pass through stdlib and third-party functions
# that use isinstance checks to detect file-like objects.
io.IOBase.register(FileProxy)
