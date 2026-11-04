import asyncio
import inspect
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Type, Union, TYPE_CHECKING

from volnux import EventBase
from volnux.signal.signals import get_signal
from volnux.event.agent.providers.base import LLMProviderAdapterBase
from volnux.event.agent.providers import LLMProviderRegistry
from volnux.flows.bridge.communications.tasks import TaskCommand
from volnux.exceptions import (
    CONTROL_FLOW_EXCEPTIONS,
    MaxReasoningStepsExceeded,
    LLMProviderError,
    ToolPermissionError,
    HallucinationDetected,
)
from ._state import (
    AgentAction,
    AgentResult,
    ToolCallRecord,
    ReasoningStepType,
    ReasoningStep,
)
from .protocol import (
    GOVERNANCE_PROTOCOL,
    Directive,
    ProtocolViolation,
    build_tool_schemas,
    parse_directive,
    trim_messages,
)

if TYPE_CHECKING:
    from volnux.flows.bridge.communications.tasks.channels.base import (
        CommandChannelBase,
    )


logger = logging.getLogger(__name__)

#: Sent after a THINK so the next model call does not end on an assistant message
#: (some providers treat that as a prefill to continue).
CONTINUE_MESSAGE = "OBSERVATION: Continue with your next directive."


def _stringify(value: Any) -> str:
    """A tool result as text for the conversation."""
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, default=str)
    except (TypeError, ValueError):
        return str(value)


def _jsonable(value: Any) -> Any:
    """``value`` if the checkpoint can hold it, else its text."""
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        return str(value)


class AgentEventBase(EventBase):
    """
    Governed, durable, auditable base class for LLM-powered agentic events.

    Subclass must declare
    ----------------------
    ``llm_provider`` (str) — provider resolved via LLMProviderRegistry
    ``llm_model`` (str) — model identifier
    ``tools`` (list) — governed tool set (EventBase subclasses)
    ``system_prompt`` (str) — domain instructions (governance preamble added automatically)
    ``prompt_version`` (str) — semver for audit trail

    Subclass must implement
    -----------------------
    ``async process(**kwargs) -> Tuple[bool, Any]``
        Calls ``await self._run_agent_loop(**kwargs)`` and returns the result.

    ``build_prompt(**kwargs) -> str``
        Constructs the user-turn prompt from injected Pipeline fields.

    Example:

        class TradeAnalysisAgent(AgentEventBase):
            llm_provider = "anthropic"
            llm_model = "claude-opus-4-20250514"
            prompt_version = "1.0.0"
            system_prompt = "You are a governed trading agent..."
            tools = [FetchOrderBookEvent, CalculateRiskEvent]
            permitted_handoffs = ["RiskAssessmentAgent"]

            async def process(self, market_data: dict, **kwargs):
                return await self._run_agent_loop(
                    market_data=market_data, **kwargs
                )

            def build_prompt(self, market_data: dict, **kwargs) -> str:
                return f"Analyse order book: {market_data}"

    Conversation and tools
    ----------------------
    Each turn the model gives one directive (``THINK``/``TOOL``/``HANDOFF``/
    ``FINISH``/``ERROR``/``PAUSE``) or, with a provider that supports it, a native
    function call. A response that breaks the protocol is answered with a correction
    (up to ``max_protocol_violations``) and does not reach the tools. A tool is a real
    event: it runs through its own executor and lifecycle, with a child command channel.

    Handoffs: ``permitted_handoffs`` lists the agents this one may hand over to; an
    empty list permits none.

    Checkpoints hold the conversation, the trace and the counters, all as plain data,
    so a resumed agent continues from the last completed step.
    """

    llm_provider: str = "openai"
    llm_model: str = "gpt-4o"
    temperature: float = 0.0
    max_tokens: int = 4096
    prompt_version: str = "1.0.0"
    system_prompt: str = ""

    tools: List[Type[Any]] = []
    permitted_handoffs: List[str] = []

    max_reasoning_steps: int = 20
    tool_timeout_seconds: float = 30.0
    llm_max_retries: int = 3
    llm_retry_delay_s: float = 1.0
    enable_hallucination_detection: bool = False
    include_trace_in_result: bool = False

    #: Token budget over the whole run; 0 disables the cap.
    max_total_tokens: int = 0
    #: Messages kept in the conversation sent to the model (see ``trim_messages``).
    max_context_messages: int = 50
    #: Options for the provider adapter (``api_key``, ``base_url``, ``timeout`` ...).
    llm_options: Dict[str, Any] = {}
    #: True: a response without a directive is a protocol violation. False: it is
    #: taken as a THINK.
    strict_protocol: bool = True
    max_protocol_violations: int = 3
    #: ReAct-style agents write ``Thought:`` lines before the directive.
    directive_preamble: bool = False

    # Run state, created per attribute by _ensure_state() (class-level defaults
    # would be shared by every instance). All of it is plain data, which is what the
    # checkpoint can hold.
    _tool_index: Dict[str, Type[Any]]
    _step_index: int
    _messages: List[Dict[str, Any]]
    _reasoning_trace: List[Dict[str, Any]]
    _total_tokens: Dict[str, int]
    _tool_call_records: List[Dict[str, Any]]
    _total_latency_ms: float
    _protocol_violations: int
    _provider_adapter: Optional[LLMProviderAdapterBase]
    _active_tool_channels: Dict[str, "CommandChannelBase"]
    _agent_start_time: float

    # Declared for EventHub manifest schema compliance.
    # Subclasses extend this dict with domain-specific parameters.
    INIT_PARAMS_SCHEMA: Dict[str, Any] = {
        "llm_provider": {
            "type": str,
            "required": False,
            "description": (
                "LLM provider resolved via LLMProviderRegistry. "
                "Declare in Pointy-Lang: TradeAnalysisAgent[llm_provider='anthropic']"
            ),
            "default": "openai",
            "enum": ["openai", "anthropic", "gemini", "mistral", "ollama", "vllm"],
        },
        "llm_model": {
            "type": str,
            "required": False,
            "description": "Model identifier passed to the provider adapter.",
            "default": "gpt-4o",
        },
        "max_reasoning_steps": {
            "type": int,
            "required": False,
            "description": "Maximum ReAct iterations before MaxReasoningStepsExceeded.",
            "default": 20,
            "validators": [
                lambda v, n, e: (
                    (_ for _ in ()).throw(ValueError(f"{n} must be >= 1"))
                    if v < 1
                    else None
                )
            ],
        },
        "temperature": {
            "type": float,
            "required": False,
            "description": (
                "LLM sampling temperature. Default 0.0 for deterministic, "
                "auditable governance. Increase for creative tasks."
            ),
            "default": 0.0,
            "validators": [
                lambda v, n, e: (
                    (_ for _ in ()).throw(ValueError(f"{n} must be in [0.0, 2.0]"))
                    if not (0.0 <= v <= 2.0)
                    else None
                )
            ],
        },
        "tool_timeout_seconds": {
            "type": float,
            "required": False,
            "description": "Per-tool-call timeout in seconds before asyncio.TimeoutError.",
            "default": 30.0,
        },
        "enable_hallucination_detection": {
            "type": bool,
            "required": False,
            "description": (
                "Enable provider-level and domain-level hallucination validation. "
                "When True, calls adapter.validate_response() and "
                "detect_hallucination() on each LLM response."
            ),
            "default": False,
        },
        "include_trace_in_result": {
            "type": bool,
            "required": False,
            "description": (
                "Include the full reasoning trace in AgentResult.reasoning_trace. "
                "Useful for downstream audit events. Increases result payload size."
            ),
            "default": False,
        },
        "max_total_tokens": {
            "type": int,
            "required": False,
            "description": (
                "Token budget cap. Agent raises MaxReasoningStepsExceeded when "
                "total tokens consumed reaches this value. 0 disables the cap."
            ),
            "default": 0,
        },
        "max_protocol_violations": {
            "type": int,
            "required": False,
            "description": (
                "Responses that break the directive protocol the agent may produce "
                "before it is stopped; each is answered with a correction."
            ),
            "default": 3,
        },
        "llm_options": {
            "type": dict,
            "required": False,
            "description": (
                "Options for the provider adapter: api_key, base_url, timeout, "
                "params (extra request parameters)."
            ),
            "default": {},
        },
    }

    async def process(self, **kwargs) -> Tuple[bool, AgentResult]:
        """
        An abstract base method that asynchronously processes input data and returns the result.

        :param kwargs: A dictionary of keyword arguments containing input data required for the
            processing logic. The exact structure and expected content of the input data will depend
            on specific subclass implementations of the method.
        :return: A tuple where the first element is a boolean indicating whether the operation was
            successful, and the second element is an instance of AgentResult encapsulating the outcome.
        """
        raise NotImplementedError("process() must be implemented by subclasses.")

    def build_prompt(self, **kwargs) -> str:
        """
        Construct the user-turn prompt from injected Pipeline fields.

        Must be implemented by every concrete agent. Called once at the
        start of each fresh (non-resumed) execution to produce the first
        user message in the LLM conversation.

        Example:

            def build_prompt(self, market_data: dict, **kwargs) -> str:
                return f"Analyse order book: {market_data}"
        """
        raise NotImplementedError(
            f"Agent '{self.__class__.__name__}' must implement build_prompt()."
        )

    def detect_hallucination(
        self,
        response: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Union[bool, Any]:
        """
        Override for domain-specific hallucination detection.
        Return ``True`` if valid, ``False`` to raise ``HallucinationDetected``.
        Maybe sync or async. Default returns ``True`` (no validation).
        """
        return True

    # ------------------------------------------------------------------
    # State
    # ------------------------------------------------------------------

    def _ensure_state(self) -> None:
        """Create the run state that is missing.

        Per attribute: a checkpoint restore may have brought back some of it, and
        what was restored must not be replaced.
        """
        if not hasattr(self, "_reasoning_trace"):
            self._reasoning_trace = []
        if not hasattr(self, "_tool_call_records"):
            self._tool_call_records = []
        if not hasattr(self, "_messages"):
            self._messages = []
        if not hasattr(self, "_step_index"):
            self._step_index = 0
        if not hasattr(self, "_total_tokens"):
            self._total_tokens = {"prompt": 0, "completion": 0, "total": 0}
        if not hasattr(self, "_total_latency_ms"):
            self._total_latency_ms = 0.0
        if not hasattr(self, "_protocol_violations"):
            self._protocol_violations = 0
        if not hasattr(self, "_provider_adapter"):
            self._provider_adapter = None
        if not hasattr(self, "_active_tool_channels"):
            self._active_tool_channels = {}

    def _repair_interrupted_tool_call(self) -> None:
        """A run that stopped between a tool request and its result left an
        assistant tool call with no answer, which providers reject. Answer it."""
        if not self._messages:
            return
        last = self._messages[-1]
        if last.get("role") == "assistant" and last.get("tool_calls"):
            for call in last["tool_calls"]:
                self._messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": call["id"],
                        "name": call["name"],
                        "content": _stringify(
                            {
                                "error": "The tool call was interrupted before it "
                                "produced a result. Call it again if you still need it."
                            }
                        ),
                    }
                )

    # ------------------------------------------------------------------
    # The loop
    # ------------------------------------------------------------------

    async def _run_agent_loop(self, **kwargs) -> Tuple[bool, Any]:
        """
        Governed ReAct reasoning loop. Called from ``process()``.

        Respects ``_pause_gate`` (PAUSE/RESUME via a command channel),
        calls ``enqueue_checkpoint()`` after every reasoning step, and
        emits soft signals at each lifecycle event.

        On crash the conversation, the trace and the counters come back from the
        checkpoint and the loop resumes at the last completed step; completed LLM
        calls are not repeated.
        """
        self._ensure_state()
        # Always rebuilt from the class: subclass overrides are reflected whatever
        # the resume state.
        self._tool_index = {cls.__name__: cls for cls in (self.__class__.tools or [])}
        self._agent_start_time = time.monotonic()
        self._validate_agent_configuration()

        if not self._messages:
            await self._initialise_messages(**kwargs)
        else:
            self._repair_interrupted_tool_call()

        self._emit_signal(
            "agent_execution_start",
            {
                "agent_class": self.__class__.__name__,
                "llm_provider": self.llm_provider,
                "llm_model": self.llm_model,
                "prompt_version": self.prompt_version,
                "max_steps": self.max_reasoning_steps,
                "tool_count": len(self._tool_index),
                "resume_step": self._step_index,
            },
        )

        result: Optional[AgentResult] = None

        try:
            while self._step_index < self.max_reasoning_steps:
                # _pause_gate is set by EventCheckPointingMixin._setup_event
                # and managed by EventCommandMixin._handle_command (PAUSE/RESUME).
                await self._pause_gate.wait()

                self._check_token_budget()
                self._trim_messages_for_context()

                llm_response = await self._call_llm_with_retry()

                try:
                    action = await self._parse_action_safe(llm_response)
                    directive = self._resolve_directive(llm_response, action)
                except ProtocolViolation as violation:
                    await self._handle_protocol_violation(llm_response, violation)
                    continue

                if self.enable_hallucination_detection:
                    await self._run_hallucination_detection(
                        llm_response, action, kwargs
                    )

                step = self._record_reasoning_step(llm_response, action, directive)

                self._emit_signal(
                    "agent_step_complete",
                    {
                        "agent_class": self.__class__.__name__,
                        "step_index": self._step_index,
                        "action": action.value,
                        "token_usage": step.token_usage,
                        "latency_ms": step.latency_ms,
                        "total_tokens": self._total_tokens["total"],
                    },
                )
                await self._on_step_complete_safe(step)
                self._step_index += 1

                if action == AgentAction.FINISH:
                    result = self._build_result(success=True, content=directive.content)
                    break

                if action == AgentAction.HANDOFF:
                    self._validate_handoff_target(directive.handoff_target)
                    result = self._build_result(
                        success=True,
                        content=directive.content,
                        handoff_target=directive.handoff_target,
                        handoff_context=directive.handoff_context,
                    )
                    break

                if action == AgentAction.ERROR:
                    result = self._build_result(
                        success=False,
                        content=directive.content
                        or "Agent reported non-recoverable error",
                        error_code=directive.error_code,
                    )
                    break

                if action == AgentAction.TOOL_CALL:
                    await self._tool_turn(llm_response, directive)
                elif action == AgentAction.PAUSE:
                    self._messages.append(
                        {
                            "role": "assistant",
                            "content": llm_response.get("content", ""),
                        }
                    )
                    self._messages.append({"role": "user", "content": CONTINUE_MESSAGE})
                    # The coordinator sets the gate again on RESUME.
                    self._pause_gate.clear()
                    logger.info(
                        "AgentEventBase '%s': PAUSE at step %d — "
                        "gate cleared, awaiting RESUME.",
                        self.__class__.__name__,
                        self._step_index,
                    )
                else:  # THINK
                    self._messages.append(
                        {
                            "role": "assistant",
                            "content": llm_response.get("content", ""),
                        }
                    )
                    self._messages.append({"role": "user", "content": CONTINUE_MESSAGE})

                # The step is complete: conversation, trace and counters are saved
                # together, so a resume continues from here.
                await self.enqueue_checkpoint()

            else:
                raise MaxReasoningStepsExceeded(
                    f"Agent '{self.__class__.__name__}' exceeded "
                    f"max_reasoning_steps={self.max_reasoning_steps}. "
                    f"Total tokens: {self._total_tokens['total']}."
                )

        except CONTROL_FLOW_EXCEPTIONS:
            # SwitchTask, StopProcessingError, SuspendTask, SkipExecutionError steer
            # the workflow; an agent must not turn them into a failed result.
            raise

        except MaxReasoningStepsExceeded as exc:
            logger.error("AgentEventBase '%s': %s", self.__class__.__name__, exc)
            self._emit_signal(
                "agent_step_limit_exceeded",
                {
                    "agent_class": self.__class__.__name__,
                    "steps_taken": self._step_index,
                    "total_tokens": self._total_tokens["total"],
                },
            )
            result = self._build_result(success=False, content=str(exc))

        except HallucinationDetected as exc:
            logger.error(
                "AgentEventBase '%s': hallucination at step %d: %s",
                self.__class__.__name__,
                self._step_index,
                exc,
            )
            self._emit_signal(
                "agent_hallucination_detected",
                {
                    "agent_class": self.__class__.__name__,
                    "step_index": self._step_index,
                    "error": str(exc),
                },
            )
            result = self._build_result(success=False, content=str(exc))

        except ToolPermissionError as exc:
            logger.error(
                "AgentEventBase '%s': GOVERNANCE VIOLATION at step %d: %s",
                self.__class__.__name__,
                self._step_index,
                exc,
            )
            self._emit_signal(
                "agent_governance_violation",
                {
                    "agent_class": self.__class__.__name__,
                    "step_index": self._step_index,
                    "violation": str(exc),
                    "severity": "HIGH",
                },
            )
            result = self._build_result(success=False, content=str(exc))

        except _ProtocolLimitExceeded as exc:
            logger.error("AgentEventBase '%s': %s", self.__class__.__name__, exc)
            result = self._build_result(success=False, content=str(exc))

        except Exception as exc:
            logger.exception(
                "AgentEventBase '%s': unexpected error at step %d",
                self.__class__.__name__,
                self._step_index,
            )
            result = self._build_result(success=False, content=str(exc))

        finally:
            await self.enqueue_checkpoint()
            total_ms = (time.monotonic() - self._agent_start_time) * 1000
            self._emit_signal(
                "agent_execution_end",
                {
                    "agent_class": self.__class__.__name__,
                    "success": result.success if result else False,
                    "steps_taken": self._step_index,
                    "total_tokens": self._total_tokens["total"],
                    "total_latency_ms": total_ms,
                },
            )
            await self._on_agent_complete_safe(result)

        if result is None:
            result = self._build_result(success=False, content="No result produced")

        return result.success, result

    def _check_token_budget(self) -> None:
        if (
            self.max_total_tokens
            and self._total_tokens["total"] >= self.max_total_tokens
        ):
            raise MaxReasoningStepsExceeded(
                f"Agent '{self.__class__.__name__}' used its token budget "
                f"({self._total_tokens['total']} >= max_total_tokens="
                f"{self.max_total_tokens})."
            )

    async def _handle_protocol_violation(
        self, response: Dict[str, Any], violation: ProtocolViolation
    ) -> None:
        """Count the violation and answer the model with a correction; past
        ``max_protocol_violations`` the agent stops."""
        self._protocol_violations += 1
        logger.warning(
            "AgentEventBase '%s': protocol violation %d/%d at step %d: %s",
            self.__class__.__name__,
            self._protocol_violations,
            self.max_protocol_violations,
            self._step_index,
            violation,
        )
        self._emit_signal(
            "agent_protocol_violation",
            {
                "agent_class": self.__class__.__name__,
                "step_index": self._step_index,
                "violation": str(violation),
                "count": self._protocol_violations,
            },
        )
        self._reasoning_trace.append(
            ReasoningStep(
                step_index=self._step_index,
                step_type=ReasoningStepType.ERROR_STATE,
                content=response.get("content", ""),
                token_usage=response.get("token_usage"),
                latency_ms=response.get("_latency_ms"),
                metadata={"protocol_violation": str(violation)},
            ).to_dict()
        )
        self._step_index += 1

        if self._protocol_violations > self.max_protocol_violations:
            await self.enqueue_checkpoint()
            raise _ProtocolLimitExceeded(
                f"Agent '{self.__class__.__name__}' broke the directive protocol "
                f"{self._protocol_violations} times (limit "
                f"{self.max_protocol_violations}): {violation}"
            )

        content = response.get("content", "")
        if content:
            self._messages.append({"role": "assistant", "content": content})
        self._messages.append(
            {
                "role": "user",
                "content": (
                    f"PROTOCOL VIOLATION: {violation} Respond with exactly one "
                    "directive (THINK, TOOL, HANDOFF, FINISH or ERROR) at the start "
                    "of your response."
                ),
            }
        )
        await self.enqueue_checkpoint()

    async def _tool_turn(self, response: Dict[str, Any], directive: Directive) -> None:
        """Run the tool the model asked for and put request and result in the
        conversation."""
        name = directive.tool_name or ""
        if name not in self._tool_index:  # before anything is added to the history
            raise ToolPermissionError(
                f"Agent '{self.__class__.__name__}' attempted undeclared tool "
                f"'{name}'. Declared: {sorted(self._tool_index)}."
            )
        call_id = directive.tool_call_id or f"call_{self._step_index - 1}"
        self._messages.append(
            {
                "role": "assistant",
                "content": response.get("content", ""),
                "tool_calls": [
                    {"id": call_id, "name": name, "arguments": directive.tool_args}
                ],
            }
        )
        tool_result = await self._execute_tool_call(
            {"tool_name": name, "tool_args": directive.tool_args}
        )
        self._messages.append(
            {
                "role": "tool",
                "tool_call_id": call_id,
                "name": name,
                "content": _stringify(tool_result),
            }
        )

    async def _initialise_messages(self, **kwargs) -> None:
        """
        Build the initial message history for a fresh execution.

        Not called on resumption — ``_messages`` is restored from the
        checkpoint snapshot written by ``SnapshotBuilder``.
        """
        system_content = self._build_governed_system_prompt()
        user_prompt = self.build_prompt(**kwargs)

        self._messages = [
            {"role": "system", "content": system_content},
            {"role": "user", "content": user_prompt},
        ]

        self._reasoning_trace.extend(
            [
                ReasoningStep(
                    step_index=-2,
                    step_type=ReasoningStepType.SYSTEM_PROMPT,
                    content=system_content,
                    metadata={
                        "prompt_version": self.prompt_version,
                        "agent_class": self.__class__.__name__,
                    },
                ).to_dict(),
                ReasoningStep(
                    step_index=-1,
                    step_type=ReasoningStepType.USER_MESSAGE,
                    content=user_prompt,
                ).to_dict(),
            ]
        )

    def _build_governed_system_prompt(self) -> str:
        """
        Prepend the governance preamble to the author's ``system_prompt``.

        Declares tool constraints, step limit, and required output format.
        Authors write-only domain content — governance is injected here.
        """
        tool_names = sorted(self._tool_index.keys())
        preamble = GOVERNANCE_PROTOCOL.format(
            agent=self.__class__.__name__,
            n=self.max_reasoning_steps,
            tools=tool_names or "NONE",
            handoffs=list(self.permitted_handoffs) or "NONE",
        )

        return f"{preamble}\n{self.system_prompt}".strip()

    def _trim_messages_for_context(self, max_messages: Optional[int] = None) -> None:
        """Keep the conversation within ``max_context_messages`` (see
        ``trim_messages``: the system prompt and the task stay, and a tool result
        is never separated from its request)."""
        limit = max_messages if max_messages is not None else self.max_context_messages
        trimmed = trim_messages(self._messages, limit, self._step_index)
        if trimmed is not self._messages:
            logger.info(
                "AgentEventBase '%s': trimmed %d messages at step %d.",
                self.__class__.__name__,
                len(self._messages) - len(trimmed),
                self._step_index,
            )
            self._messages = trimmed

    # ------------------------------------------------------------------
    # The model
    # ------------------------------------------------------------------

    def _get_provider_adapter(self) -> LLMProviderAdapterBase:
        """Lazily resolve and cache the provider adapter."""
        self._ensure_state()
        if self._provider_adapter is None:
            self._provider_adapter = LLMProviderRegistry.resolve(
                self.llm_provider, **dict(self.llm_options or {})
            )
        return self._provider_adapter

    async def _call_llm_with_retry(self) -> Dict[str, Any]:
        """Call the LLM, backing off exponentially between attempts. An error the
        provider says is not worth retrying (bad key, bad request) ends it at once."""
        adapter = self._get_provider_adapter()
        tool_schemas = self._build_tool_schemas()
        attempts = max(1, self.llm_max_retries)
        last_exc: Optional[Exception] = None

        for attempt in range(1, attempts + 1):
            try:
                t0 = time.monotonic()
                response = await adapter.complete(
                    messages=list(self._messages),
                    model=self.llm_model,
                    temperature=self.temperature,
                    max_tokens=self.max_tokens,
                    tools=tool_schemas or None,
                )
                if not isinstance(response, dict):
                    raise LLMProviderError(
                        f"Adapter '{self.llm_provider}' returned "
                        f"{type(response).__name__}, expected a dict.",
                        retryable=False,
                    )
                latency_ms = (time.monotonic() - t0) * 1000
                usage = response.get("token_usage") or {}
                self._total_tokens["prompt"] += usage.get("prompt", 0)
                self._total_tokens["completion"] += usage.get("completion", 0)
                self._total_tokens["total"] += usage.get("total", 0)
                self._total_latency_ms += latency_ms
                response["_latency_ms"] = latency_ms
                return response

            except LLMProviderError as exc:
                last_exc = exc
                if not exc.retryable:
                    raise
                if attempt < attempts:
                    delay = self.llm_retry_delay_s * (2 ** (attempt - 1))
                    logger.warning(
                        "AgentEventBase '%s': LLM failed (attempt %d/%d), "
                        "retrying in %.1fs: %s",
                        self.__class__.__name__,
                        attempt,
                        attempts,
                        delay,
                        exc,
                    )
                    await asyncio.sleep(delay)

        raise LLMProviderError(
            f"LLM '{self.llm_provider}' failed after {attempts} "
            f"attempts. Last error: {last_exc}",
            retryable=False,
            status_code=getattr(last_exc, "status_code", None),
        )

    def _build_tool_schemas(self) -> List[Dict[str, Any]]:
        """Function-calling schemas from each tool's ``INIT_PARAMS_SCHEMA`` (the
        schema the EventHub manifest declares)."""
        return build_tool_schemas(self.tools)

    # ------------------------------------------------------------------
    # Tools
    # ------------------------------------------------------------------

    def _create_child_channel(
        self, tool_task_id: str
    ) -> Optional["CommandChannelBase"]:
        """
        A command channel for a tool call: the same kind as the agent's, scoped to
        ``tool_task_id``, with its own listener lifecycle, so completing the tool
        tears down only this channel and not the agent's.

        None when the agent has no channel (standalone execution) or one cannot be
        created: the tool then runs without command support rather than not at all.
        """
        channel = self._command_channel
        if channel is None:
            return None
        try:
            create_child = getattr(channel, "create_child", None)
            if create_child is not None:
                return create_child(tool_task_id)  # type: ignore[no-any-return]
            return type(channel)(tool_task_id)
        except Exception as exc:
            logger.warning(
                "AgentEventBase '%s': failed to create child channel "
                "for tool '%s': %s. Tool will run without command channel.",
                self.__class__.__name__,
                tool_task_id,
                exc,
            )
            return None

    @staticmethod
    def _tool_executor(tool: Any) -> Tuple[Any, bool]:
        """The executor a tool runs on, and whether this call owns (must shut down)
        it. Standard pools are the shared ones, as for any flow."""
        from volnux.executors.shared import get_shared_pool

        executor = tool.get_task_executor()
        if not inspect.isclass(executor):
            return executor, False
        shared = get_shared_pool(executor)
        if shared is not None:
            return shared, False
        return executor(**(tool.get_executor_context() or {})), True

    async def _execute_tool_call(self, llm_response: Dict[str, Any]) -> Any:
        """
        Execute a governed tool call as a full EventBase lifecycle invocation.

        The tool is a first-class EventBase instance with its own task id, the
        agent's checkpoint manager (so its state is a child of the agent's) and a
        child command channel. It runs on its own configured executor, exactly as it
        would in a flow (``BaseFlow._dispatch_to_executor``): the async executor on
        this loop, thread/process/remote executors through the synchronous wrapper.
        The call is bounded by ``tool_timeout_seconds``.

        A failing, timing-out or raising tool is reported to the model as
        ``{"error": ...}`` so it can react; control-flow exceptions
        (``SwitchTask``, ``SuspendTask`` ...) are not swallowed.

        Raises ``ToolPermissionError`` for a tool that is not declared.
        """
        from volnux.flows.base import BaseFlow

        tool_name = llm_response.get("tool_name", "")
        tool_args = llm_response.get("tool_args", {}) or {}
        step_index = self._step_index - 1  # the loop has counted this step already

        if tool_name not in self._tool_index:
            raise ToolPermissionError(
                f"Agent '{self.__class__.__name__}' attempted undeclared tool "
                f"'{tool_name}'. Declared: {sorted(self._tool_index)}."
            )
        tool_class = self._tool_index[tool_name]

        # Intent is recorded before the tool runs, so it survives a crash.
        record = ToolCallRecord(
            tool_class_name=tool_name, tool_args=_jsonable(tool_args)
        )
        self._tool_call_records.append(record.to_dict())
        record_index = len(self._tool_call_records) - 1
        await self.enqueue_checkpoint()

        self._emit_signal(
            "agent_tool_call_start",
            {
                "agent_class": self.__class__.__name__,
                "tool_name": tool_name,
                "tool_args": tool_args,
                "step_index": step_index,
            },
        )

        tool_result: Any = None
        tool_error: Optional[str] = None
        t0 = time.monotonic()
        tool_task_id = f"{self._task_id}:tool:{tool_name}:step:{step_index}"
        executor: Any = None
        owns_executor = False

        try:
            child_channel = self._create_child_channel(tool_task_id)
            tool_instance = tool_class(
                execution_context=self._execution_context,
                task_id=tool_task_id,
                checkpoint_manager=self.checkpoint_manager,
                command_channel=child_channel,
            )
            self._register_tool_channel(tool_task_id, child_channel)

            executor, owns_executor = self._tool_executor(tool_instance)
            future = BaseFlow._dispatch_to_executor(executor, tool_instance, tool_args)
            event_result = await asyncio.wait_for(
                future, timeout=self.tool_timeout_seconds
            )

            if event_result is None:
                tool_error = f"Tool '{tool_name}' returned None"
            elif event_result.error:
                tool_error = f"Tool '{tool_name}' failed: {event_result.content}"
            else:
                tool_result = event_result.content

        except CONTROL_FLOW_EXCEPTIONS:
            raise
        except asyncio.TimeoutError:
            tool_error = (
                f"Tool '{tool_name}' timed out after {self.tool_timeout_seconds}s."
            )
        except Exception as exc:
            tool_error = f"Tool '{tool_name}' raised: {exc}"
            logger.exception(
                "AgentEventBase '%s': tool '%s' raised at step %d",
                self.__class__.__name__,
                tool_name,
                step_index,
            )
        finally:
            self._deregister_tool_channel(tool_task_id)
            if owns_executor and executor is not None:
                try:
                    executor.shutdown(wait=False)
                except Exception:
                    logger.debug("Tool executor shutdown failed", exc_info=True)

        if tool_error:
            logger.error("AgentEventBase '%s': %s", self.__class__.__name__, tool_error)

        latency_ms = (time.monotonic() - t0) * 1000
        record.result = _jsonable(tool_result)
        record.error = tool_error
        record.completed_at = datetime.now(timezone.utc).isoformat()
        record.latency_ms = latency_ms
        self._tool_call_records[record_index] = record.to_dict()

        self._reasoning_trace.append(
            ReasoningStep(
                step_index=step_index,
                step_type=ReasoningStepType.TOOL_RESULT,
                content=_jsonable(tool_result if tool_error is None else tool_error),
                tool_name=tool_name,
                tool_args=_jsonable(tool_args),
                tool_result=_jsonable(tool_result),
                latency_ms=latency_ms,
                metadata={"error": tool_error} if tool_error else {},
            ).to_dict()
        )

        await self.enqueue_checkpoint()
        await self._on_tool_result_safe(record)

        self._emit_signal(
            "agent_tool_call_end",
            {
                "agent_class": self.__class__.__name__,
                "tool_name": tool_name,
                "success": tool_error is None,
                "latency_ms": latency_ms,
                "step_index": step_index,
            },
        )

        return tool_result if tool_error is None else {"error": tool_error}

    def _register_tool_channel(
        self,
        tool_task_id: str,
        channel: Optional[Any],
    ) -> None:
        """Register an active tool channel so ``_forward_tool_command`` can route
        coordinator messages to it."""
        self._ensure_state()
        if channel is not None:
            self._active_tool_channels[tool_task_id] = channel

    def _deregister_tool_channel(self, tool_task_id: str) -> None:
        """Remove a finished tool's channel (always runs, success or failure)."""
        self._ensure_state()
        self._active_tool_channels.pop(tool_task_id, None)

    async def _forward_tool_command(
        self,
        tool_task_id: str,
        command: Any,
    ) -> None:
        """
        Forward a coordinator command to an active tool's child channel.

        The coordinator addresses a tool through the agent's channel ("tell tool
        {tool_task_id} to PAUSE"); the agent forwards it here. A command for a tool
        that has finished is dropped: it is no longer running.
        """
        self._ensure_state()
        child_channel = self._active_tool_channels.get(tool_task_id)
        if child_channel is None:
            logger.debug(
                "AgentEventBase '%s': received command for completed "
                "tool '%s' — dropping.",
                self.__class__.__name__,
                tool_task_id,
            )
            return

        try:
            await child_channel.send_command(command)
            logger.debug(
                "AgentEventBase '%s': forwarded command '%s' to tool '%s'.",
                self.__class__.__name__,
                getattr(command, "command_type", command),
                tool_task_id,
            )
        except Exception as exc:
            logger.warning(
                "AgentEventBase '%s': failed to forward command to tool '%s': %s",
                self.__class__.__name__,
                tool_task_id,
                exc,
            )

    async def _handle_command(self, command: TaskCommand) -> None:
        if command.task_id != self._task_id:
            # Command is addressed to a tool, not to the agent.
            await self._forward_tool_command(command.task_id, command)
            return

        await super()._handle_command(command)

    # ------------------------------------------------------------------
    # Handoff governance
    # ------------------------------------------------------------------

    def _validate_handoff_target(self, target: Optional[str]) -> None:
        """Raise ``ToolPermissionError`` unless ``target`` is in
        ``permitted_handoffs``. An agent with no permitted handoffs may hand over to
        nobody."""
        if not target:
            raise ToolPermissionError(
                f"Agent '{self.__class__.__name__}': HANDOFF produced no target."
            )
        if target not in (self.permitted_handoffs or []):
            raise ToolPermissionError(
                f"Agent '{self.__class__.__name__}' attempted undeclared "
                f"handoff to '{target}'. Permitted: {list(self.permitted_handoffs)}."
            )

    def handoff_to(
        self,
        target_agent_class_name: str,
        context: Dict[str, Any],
        descriptor: int = 1,
    ) -> None:
        """
        Governed delegation to another agent via Pointy-Lang descriptor.

        Uses the inherited ``goto()`` from EventBase to emit a descriptor
        that routes the Pointy-Lang graph to the correct next event.

        Example Pointy-Lang:

            TradeAnalysisAgent (
                0 -> HandleFailure,
                1 -> RiskAssessmentAgent,
                2 -> ComplianceAuditAgent,
            )

        Called inside ``process()`` or a tool result handler::

            self.handoff_to("RiskAssessmentAgent", context=result, descriptor=1)
        """
        self._validate_handoff_target(target_agent_class_name)
        # goto() is inherited from EventBase — raises SwitchTask to route the graph
        self.goto(
            descriptor=descriptor,
            result_success=True,
            result={
                "handoff_target": target_agent_class_name,
                "context": context,
            },
        )

    # ------------------------------------------------------------------
    # Understanding a response
    # ------------------------------------------------------------------

    def _directive_for(self, response: Dict[str, Any]) -> Directive:
        """What the model's response says. A native function call from the provider
        is taken as it is; otherwise the text must follow the directive protocol
        (``strict_protocol=False`` takes a response without one as a THINK).

        Raises ``ProtocolViolation`` in strict mode."""
        content = str(response.get("content", "") or "")
        native = response.get("tool_name")
        if native:
            return Directive(
                AgentAction.TOOL_CALL,
                content=content,
                tool_name=native,
                tool_args=dict(response.get("tool_args") or {}),
                tool_call_id=response.get("tool_call_id"),
            )
        try:
            return parse_directive(content, allow_preamble=self.directive_preamble)
        except ProtocolViolation:
            if self.strict_protocol:
                raise
            return Directive(AgentAction.THINK, content=content)

    def _resolve_directive(
        self, response: Dict[str, Any], action: AgentAction
    ) -> Directive:
        """The directive for ``action``. When ``parse_action`` was overridden and
        chose an action the text does not carry, the response's own fields are used."""
        try:
            directive = self._directive_for(response)
        except ProtocolViolation:
            directive = None
        if directive is not None and directive.action == action:
            return directive
        return Directive(
            action,
            content=str(response.get("content", "") or ""),
            tool_name=response.get("tool_name"),
            tool_args=dict(response.get("tool_args") or {}),
            tool_call_id=response.get("tool_call_id"),
            handoff_target=response.get("handoff_target"),
            handoff_context=dict(response.get("handoff_context") or {}),
        )

    async def _parse_action_safe(self, response: Dict[str, Any]) -> AgentAction:
        try:
            result = self.parse_action(response)
            if asyncio.iscoroutine(result) or inspect.isawaitable(result):
                return await result  # type: ignore[no-any-return]
            return result  # type: ignore[no-any-return]
        except (ProtocolViolation, *CONTROL_FLOW_EXCEPTIONS):
            raise
        except Exception as exc:
            logger.warning(
                "AgentEventBase '%s': parse_action raised '%s', using default.",
                self.__class__.__name__,
                exc,
            )
            return self._default_parse_action(response)

    def parse_action(self, response: Dict[str, Any]) -> Union[AgentAction, Any]:
        """
        Override to implement custom action parsing.
        May return an ``AgentAction`` or a coroutine resolving to one, or raise
        ``ProtocolViolation``. Default reads the directive (or native call).
        """
        return self._default_parse_action(response)

    def _default_parse_action(self, response: Dict[str, Any]) -> AgentAction:
        return self._directive_for(response).action

    async def _run_hallucination_detection(
        self,
        response: Dict[str, Any],
        action: AgentAction,
        context: Dict[str, Any],
    ) -> None:
        adapter = self._get_provider_adapter()
        if not await adapter.validate_response(response, context):
            raise HallucinationDetected(
                f"Provider validation failed for '{self.__class__.__name__}' "
                f"at step {self._step_index}."
            )
        domain = self.detect_hallucination(response, context)
        if asyncio.iscoroutine(domain) or inspect.isawaitable(domain):
            domain = await domain
        if not domain:
            raise HallucinationDetected(
                f"Domain validation failed for '{self.__class__.__name__}' "
                f"at step {self._step_index}. "
                f"Content: {str(response.get('content', ''))[:200]}"
            )

    def _record_reasoning_step(
        self,
        llm_response: Dict[str, Any],
        action: AgentAction,
        directive: Optional[Directive] = None,
    ) -> ReasoningStep:
        step_type_map = {
            AgentAction.THINK: ReasoningStepType.ASSISTANT_THINK,
            AgentAction.TOOL_CALL: ReasoningStepType.TOOL_REQUEST,
            AgentAction.FINISH: ReasoningStepType.FINAL_ANSWER,
            AgentAction.HANDOFF: ReasoningStepType.HANDOFF_REQUEST,
            AgentAction.ERROR: ReasoningStepType.ERROR_STATE,
            AgentAction.PAUSE: ReasoningStepType.ASSISTANT_THINK,
        }
        tool_name = directive.tool_name if directive else llm_response.get("tool_name")
        tool_args = directive.tool_args if directive else llm_response.get("tool_args")
        step = ReasoningStep(
            step_index=self._step_index,
            step_type=step_type_map.get(action, ReasoningStepType.ASSISTANT_THINK),
            content=llm_response.get("content", ""),
            token_usage=llm_response.get("token_usage"),
            latency_ms=llm_response.get("_latency_ms"),
            tool_name=tool_name,
            tool_args=_jsonable(tool_args) if tool_args is not None else None,
            metadata={
                "action": action.value,
                "finish_reason": llm_response.get("finish_reason", ""),
                "model": self.llm_model,
                "provider": self.llm_provider,
                "prompt_version": self.prompt_version,
            },
        )
        self._reasoning_trace.append(step.to_dict())
        return step

    def _build_result(
        self,
        success: bool,
        content: Any,
        handoff_target: Optional[str] = None,
        **metadata: Any,
    ) -> AgentResult:
        self._ensure_state()
        return AgentResult(
            success=success,
            content=content,
            reasoning_trace=(
                [ReasoningStep.from_dict(d) for d in self._reasoning_trace]
                if self.include_trace_in_result
                else []
            ),
            total_tokens=self._total_tokens["total"],
            total_latency_ms=self._total_latency_ms,
            steps_taken=self._step_index,
            handoff_target=handoff_target,
            metadata={
                "agent_class": self.__class__.__name__,
                "llm_provider": self.llm_provider,
                "llm_model": self.llm_model,
                "prompt_version": self.prompt_version,
                "tool_calls": len(self._tool_call_records),
                "prompt_tokens": self._total_tokens["prompt"],
                "completion_tokens": self._total_tokens["completion"],
                "protocol_violations": self._protocol_violations,
                **{k: v for k, v in metadata.items() if v not in (None, {})},
            },
        )

    def _validate_agent_configuration(self) -> None:
        """Validate class configuration at loop entry, not at import time."""
        name = self.__class__.__name__
        if not self.system_prompt:
            raise TypeError(f"Agent '{name}' has no system_prompt.")
        if not (0.0 <= self.temperature <= 2.0):
            raise TypeError(
                f"Agent '{name}': temperature must be in [0.0, 2.0], got "
                f"{self.temperature}."
            )
        if self.max_reasoning_steps < 1:
            raise TypeError(f"Agent '{name}': max_reasoning_steps must be >= 1.")
        if self.max_total_tokens < 0:
            raise TypeError(f"Agent '{name}': max_total_tokens must be >= 0.")
        if self.max_protocol_violations < 0:
            raise TypeError(f"Agent '{name}': max_protocol_violations must be >= 0.")
        if self.max_context_messages < 4:
            raise TypeError(f"Agent '{name}': max_context_messages must be >= 4.")

    # ------------------------------------------------------------------
    # Hooks (override in subclasses)
    # ------------------------------------------------------------------

    def on_step_complete(self, step: ReasoningStep) -> None:
        """Called after each reasoning step. Default is no-op."""

    def on_tool_result(self, record: ToolCallRecord) -> None:
        """Called after each tool call. Default is no-op."""

    def on_agent_complete(self, result: Optional[AgentResult]) -> None:
        """Called at terminal state. Default is no-op."""

    async def _run_hook(self, name: str, *args: Any) -> None:
        """Run a hook; a hook that raises is logged, never fatal (control-flow
        exceptions excepted)."""
        try:
            r = getattr(self, name)(*args)
            if asyncio.iscoroutine(r) or inspect.isawaitable(r):
                await r
        except CONTROL_FLOW_EXCEPTIONS:
            raise
        except Exception as exc:
            logger.warning(
                "AgentEventBase '%s': %s raised: %s", self.__class__.__name__, name, exc
            )

    async def _on_step_complete_safe(self, step: ReasoningStep) -> None:
        await self._run_hook("on_step_complete", step)

    async def _on_tool_result_safe(self, record: ToolCallRecord) -> None:
        await self._run_hook("on_tool_result", record)

    async def _on_agent_complete_safe(self, result: Optional[AgentResult]) -> None:
        await self._run_hook("on_agent_complete", result)

    def _emit_signal(self, signal_name: str, data: Dict[str, Any]) -> None:
        """Emit a soft signal. Fails silently — loop continues regardless."""
        try:
            sig = get_signal(signal_name)
            if sig:
                sig.emit(sender=self.__class__, **data)
        except Exception:
            logger.debug("Signal '%s' failed", signal_name, exc_info=True)


class _ProtocolLimitExceeded(RuntimeError):
    """The model broke the directive protocol more often than allowed."""
