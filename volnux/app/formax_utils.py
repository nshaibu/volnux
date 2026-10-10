import dataclasses
import inspect
from enum import Enum
from typing import Any, Dict, List, Type, get_args, get_origin

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
from formax.typing import is_mini_annotated, MISSING
from formax import Attrib, MiniAnnotated, BaseModel


jsonschema_datatypes_map: Dict[type, str] = {
    str: "string",
    int: "integer",
    float: "number",
    bool: "boolean",
    list: "array",
    dict: "object",
    None: "null",
}


def extract_formax_constraints(
    model_class: Type[BaseModel],
) -> Dict[str, Dict[str, Any]]:
    """Extract validation constraints from a Formax model's fields.

    Walks the model's dataclass fields and MiniAnnotated type annotations
    to extract Attrib constraints. Returns a dict mapping field names
    to their OpenAPI-compatible constraint dictionaries.

    Args:
        model_class: A Formax model class (dataclass with MiniAnnotated fields).

    Returns:
        Dict mapping field_name -> OpenAPI schema constraints.
        Example: {"name": {"minLength": 1, "maxLength": 255}}

    Example:
        >>> class User(BaseModel):
        ...     name: MiniAnnotated[str, Attrib(min_length=1, max_length=255)]
        ...     age: MiniAnnotated[int, Attrib(ge=0, lt=120)]
        ...
        >>> extract_formax_constraints(User)
        {'name': {'minLength': 1, 'maxLength': 255}, 'age': {'minimum': 0, 'maximum': 119}}
    """
    constraints = {}

    if not dataclasses.is_dataclass(model_class):
        return constraints

    for field in dataclasses.fields(model_class):
        field_constraints = _extract_field_constraints(field)
        if field_constraints:
            constraints[field.name] = field_constraints

    return constraints


def _extract_field_constraints(field: dataclasses.Field) -> Dict[str, Any]:
    """Extract constraints from a single dataclass field's type annotation.

    Handles MiniAnnotated types with Attrib metadata.
    """
    constraints = {}
    field_annotation = field.type

    if not is_mini_annotated(field_annotation):
        field_annotation = MiniAnnotated[field_annotation, Attrib()]

    field_type = field_annotation.__args__[0]
    attrib: Attrib = field_annotation.__metadata__[0]

    actual_type = get_origin(field_type) or field_type

    if inspect.isclass(actual_type):
        if issubclass(actual_type, Enum):
            constraints["type"] = "string"
            constraints["enum"] = [member.value for member in actual_type]
            return constraints

    field_type_str = jsonschema_datatypes_map.get(actual_type, "object")

    constraints["type"] = field_type_str

    if attrib is None:
        return constraints

    if attrib.min_length is not None:
        constraints["minLength"] = attrib.min_length
    if attrib.max_length is not None:
        constraints["maxLength"] = attrib.max_length
    if attrib.pattern is not None:
        constraints["pattern"] = attrib.pattern

    if attrib.ge is not None:
        constraints["minimum"] = attrib.ge
    if attrib.gt is not None:
        constraints["exclusiveMinimum"] = attrib.gt
    if attrib.le is not None:
        constraints["maximum"] = attrib.le
    if attrib.lt is not None:
        constraints["exclusiveMaximum"] = attrib.lt
    if attrib.has_default():
        default_value = (
            field.default if field.default is not MISSING else field.default_factory()
        )
        if not hasattr(default_value, "kind"):
            constraints["default"] = default_value
    if attrib.help_text is not None:
        constraints["description"] = attrib.help_text

    return constraints


def patch_openapi_with_formax_constraints(
    app: FastAPI,
    model_constraints: Dict[Type, Dict[str, Dict[str, Any]]],
) -> None:
    """Patch FastAPI's generated OpenAPI schema with Formax field constraints.

    Modifies the OpenAPI schema in-place to add constraints from Formax
    models to their corresponding request/response schemas.

    Args:
        app: The FastAPI application instance.
        model_constraints: Dict mapping Formax model classes to their
                          extracted field constraints.
                          Use extract_formax_constraints() to generate.

    Example:
        >>> from volnux.models import Workflow, User, Team
        >>> constraints = {
        ...     Workflow: extract_formax_constraints(Workflow),
        ...     User: extract_formax_constraints(User),
        ...     Team: extract_formax_constraints(Team),
        ... }
        >>> patch_openapi_with_formax_constraints(app, constraints)
    """
    # Build a lookup from model class name to constraints
    constraint_map: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for model_class, field_constraints in model_constraints.items():
        constraint_map[model_class.__name__] = field_constraints

    def custom_openapi():
        if app.openapi_schema:
            return app.openapi_schema

        openapi_schema = get_openapi(
            title=app.title,
            version=app.version,
            description=app.description,
            routes=app.routes,
        )

        # Walk through the schema and patch matching schemas
        schemas = openapi_schema.get("components", {}).setdefault("schemas", {})
        for schema_name, schema in schemas.items():
            # Match schema names to model class names
            if schema_name in constraint_map:
                _apply_constraints_to_schema(schema, constraint_map[schema_name])

            # Also check if the schema name appears in the constraint map keys
            # (handles cases where FastAPI adds prefixes/suffixes)
            for model_name, field_constraints in constraint_map.items():
                if model_name in schema_name:
                    _apply_constraints_to_schema(schema, field_constraints)
                    break

        # Add schemas for models not yet present in the OpenAPI output
        existing_schema_names = set(schemas.keys())
        for model_name, field_constraints in constraint_map.items():
            already_covered = any(
                model_name == name or model_name in name
                for name in existing_schema_names
            )
            if not already_covered:
                new_schema: Dict[str, Any] = {
                    "type": "object",
                    "properties": {
                        field: {k: v for k, v in constraints.items() if k != "type"}
                        for field, constraints in field_constraints.items()
                    },
                }
                _apply_constraints_to_schema(new_schema, field_constraints)
                schemas[model_name] = new_schema

        app.openapi_schema = openapi_schema
        return app.openapi_schema

    app.openapi = custom_openapi


def _apply_constraints_to_schema(
    schema: Dict[str, Any],
    field_constraints: Dict[str, Dict[str, Any]],
) -> None:
    """Apply field constraints to a single OpenAPI schema object.

    Modifies the schema's properties in-place.

    Args:
        schema: The OpenAPI schema objects to modify.
        field_constraints: Dict mapping field names to constraint dicts.
    """
    properties = schema.get("properties", {})
    if not properties:
        return

    for field_name, constraints in field_constraints.items():
        if field_name in properties:
            properties[field_name].update(constraints)

    # Also handle required fields
    required = schema.get("required", [])
    for field_name, constraints in field_constraints.items():
        # If a field has a non-null constraint (min_length >= 1, ge, etc.)
        # and isn't Optional, it should be required
        is_optional = (
            "nullable" in properties.get(field_name, {})
            or properties.get(field_name, {}).get("type") == "null"
        )
        if not is_optional and field_name not in required:
            # Only add to required if the field is in properties
            if field_name in properties:
                required.append(field_name)
    if required:
        schema["required"] = required


def register_formax_model_constraints(
    app: FastAPI,
    *model_classes: Type,
) -> None:
    """Convenience function to extract and patch constraints in one call.

    Args:
        app: The FastAPI application instance.
        *model_classes: Formax model classes to extract constraints from.

    Example:
        >>> from volnux.models import Workflow, User, Team
        >>> register_formax_model_constraints(app, Workflow, User, Team)
    """
    constraints = {}
    for model_class in model_classes:
        field_constraints = extract_formax_constraints(model_class)
        if field_constraints:
            constraints[model_class] = field_constraints

    if constraints:
        patch_openapi_with_formax_constraints(app, constraints)
