import typing

from pydantic_mini import BaseModel


class Payload(BaseModel):
    type: str
    correlation_id: str
    event_name: str
    args: dict
    client_id: str
    timeout: typing.Optional[int]
    timestamp: float
    hmac: str


class QueryEventPayload(BaseModel):
    type: str = "event_query"
    event_name: str
    client_id: str


class QueryEventResponse(BaseModel):
    type: str = "event_availability"
    event_name: str
    available: bool
    message: str
    metadata: dict


class TaskExecutionErrorResponse(BaseModel):
    type: str = "error"
    correlation_id: str
    status: str = "error"
    message: str
    code: str
    timestamp: float


class TaskExecutionSuccessResponse(BaseModel):
    type: str = "task_response"
    correlation_id: str
    status: str = "success"
    result: dict
    completed_at: float
    hmac: str
