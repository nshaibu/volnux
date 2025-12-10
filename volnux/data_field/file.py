import os
import typing
from .base_input import InputDataField
from .file_proxy import FileProxy
from volnux.constants import EMPTY
from volnux import default_batch_processors as batch_defaults


T = typing.TypeVar("T")

if typing.TYPE_CHECKING:
    from volnux.pipeline import Pipeline


class FileInputDataField(InputDataField):

    def __init__(
        self,
        path: typing.Union[str, os.PathLike, None] = None,
        required: bool = False,
        chunk_size: int = batch_defaults.DEFAULT_CHUNK_SIZE,
        mode: str = "r",
        encoding: typing.Optional[str] = None,
    ):
        self.mode = mode
        self.encoding = encoding

        super().__init__(
            name=typing.cast(str, path),
            required=required,
            data_type=(str, os.PathLike),
            default=EMPTY,
            batch_size=chunk_size,
            batch_processor=batch_defaults.file_stream_batch_processor,
        )

    def __set__(self, instance: "Pipeline", value: typing.Any) -> None:
        """
        Set the file path, validating that it exists and is a file.

        Args:
            instance: The instance to set the value on
            value: The file path

        Raises:
            ValueError: If the path doesn't exist or is not a file
        """
        if value is not None and not os.path.isfile(value):
            raise ValueError(f"Path '{value}' is not a file or does not exist")

        super().__set__(instance, value)

    def __get__(
        self, instance: "Pipeline", owner: typing.Optional[object] = None
    ) -> typing.Optional[FileProxy]:
        """
        Get an open file handle for the file path.

        Args:
            instance: The instance to get the value from
            owner: The class that owns this descriptor

        Returns:
            An open file object or None if no path is set

        Note:
            The caller is responsible for closing the file when done
        """
        if instance is None:
            return self  # type: ignore

        value: typing.Union[str, os.PathLike] = super().__get__(instance, owner)

        if value:
            kwargs: typing.Dict[str, typing.Any] = {}
            if "b" not in self.mode and self.encoding is not None:
                kwargs["encoding"] = self.encoding

            return FileProxy(file_path=value, mode=self.mode, **kwargs)

        return None
