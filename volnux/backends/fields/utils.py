from formax import ValidationError


def formax_null_validator(instance, value):
    if value is None:
        raise ValidationError("Field cannot be null")
