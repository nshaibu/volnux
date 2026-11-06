import typing
import threading

from volnux.utils import generate_unique_id, get_obj_klass_import_str


class ObjectIdentityMixin:
    """
    Mixin class to provide objects with a unique identity and state management.

    This class is designed to give derived classes a unique identifier, methods
    to manage and change this identifier, and methods to handle serialization and
    deserialization of the object's state. The unique identifier is generated upon
    initialization and can be modified with proper safeguards.

    :ivar id: The unique identifier for the object.
    :type id: str

    Subclasses must implement:
        get_state() -> dict — return serializable state, must include '_id'.
        set_state(state: dict) — restore state from dict, must restore '_id'.
    """

    _id: str

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        cls._objectid_lock = threading.Lock()

    def __post_init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        # super().__init__(*args, **kwargs)
        # self._objectid_lock = threading.Lock()
        generate_unique_id(self, lock=self._objectid_lock)

    @property
    def id(self) -> str:
        return generate_unique_id(self, lock=self._objectid_lock)

    def change_object_id(self, new_id: str) -> None:
        if not isinstance(new_id, str) or not new_id.strip():
            raise ValueError("new_id must be a non-empty string.")
        with self._objectid_lock:
            try:
                self._id = new_id
            except AttributeError:
                object.__setattr__(self, "_id", new_id)

    @property
    def __object_import_str__(self) -> str:
        return get_obj_klass_import_str(self)

    def get_state(self) -> typing.Dict[str, typing.Any]:
        if hasattr(self, "__get_formax_state__"):
            return self.__get_formax_state__()
        raise NotImplementedError()

    def set_state(self, state: typing.Dict[str, typing.Any]) -> None:
        if hasattr(self, "__set_formax_state__"):
            self.__set_formax_state__(state)
            return
        raise NotImplementedError()

    def __setstate__(self, state: typing.Dict[str, typing.Any]) -> None:
        # self._objectid_lock = threading.Lock()
        self.set_state(state)

    def __getstate__(self) -> typing.Dict[str, typing.Any]:
        return self.get_state()
