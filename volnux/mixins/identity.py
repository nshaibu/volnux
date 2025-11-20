import typing

from volnux.utils import generate_unique_id, get_obj_klass_import_str


class ObjectIdentityMixin:
    def __init__(
        self, *args: typing.Tuple[typing.Any], **kwargs: typing.Dict[str, typing.Any]
    ) -> None:
        generate_unique_id(self)

    @property
    def id(self) -> str:
        return generate_unique_id(self)

    @property
    def __object_import_str__(self) -> typing.Type[typing.Any]:
        return get_obj_klass_import_str(self)  # type: ignore

    def get_state(self) -> typing.Dict[str, typing.Any]:
        raise NotImplementedError()

    def set_state(self, state: typing.Dict[str, typing.Any]) -> None:
        raise NotImplementedError()

    def __setstate__(self, state: typing.Dict[str, typing.Any]) -> None:
        self.set_state(state)

    def __getstate__(self) -> typing.Dict[str, typing.Any]:
        return self.get_state()
