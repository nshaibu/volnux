import orjson
from formax.formatters import DictModelFormatter


class OrJSONModelFormatter(DictModelFormatter):
    format_name = "orjson"

    def encode(self, _type, obj: str):
        return super().encode(_type, orjson.loads(obj))

    def decode(self, instance) -> str:
        data = orjson.dumps(super().decode(instance), default=str)
        return data.decode("utf-8")
