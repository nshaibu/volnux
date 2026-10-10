import jwt

from fastapi import Request
from fastapi.responses import JSONResponse
from formax import ValidationError

from .app import get_current_app
from volnux.exceptions import (
    ObjectDoesNotExist,
    ObjectExistError,
    ObjectProtectedError,
    ValidationError as VolnuxValidationError,
)


app = get_current_app()


@app.exception_handler(ObjectDoesNotExist)
async def handle_not_found(request: Request, exc: ObjectDoesNotExist):
    return JSONResponse(
        status_code=404,
        content={
            "status": "error",
            "error": {"code": "NOT_FOUND", "message": str(exc)},
        },
    )


@app.exception_handler(ObjectExistError)
async def handle_conflict(request: Request, exc: ObjectExistError):
    return JSONResponse(
        status_code=409,
        content={"status": "error", "error": {"code": "CONFLICT", "message": str(exc)}},
    )


@app.exception_handler(ObjectProtectedError)
async def handle_protected(request: Request, exc: ObjectProtectedError):
    return JSONResponse(
        status_code=409,
        content={
            "status": "error",
            "error": {"code": "PROTECTED", "message": str(exc)},
        },
    )


@app.exception_handler(VolnuxValidationError)
async def handle_validation(request: Request, exc: VolnuxValidationError):
    return JSONResponse(
        status_code=422,
        content={
            "status": "error",
            "error": {"code": "VALIDATION_ERROR", "message": str(exc)},
        },
    )


@app.exception_handler(ValidationError)
async def formax_validation_handler(request: Request, exc: ValidationError):
    return JSONResponse(
        status_code=422,
        content=exc.errors(),
    )
