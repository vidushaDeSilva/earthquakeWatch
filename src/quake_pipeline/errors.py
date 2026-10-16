"""Define safe pipeline failures without embedding response bodies or credentials."""


class PipelineError(Exception):
    """An operational failure whose code and message are safe to print."""

    code = "PIPELINE_ERROR"


class ConfigurationError(PipelineError):
    """A required configuration value is missing or invalid."""

    code = "CONFIGURATION_ERROR"


class ContractError(PipelineError):
    """A source response or saved manifest violates its contract."""

    code = "CONTRACT_ERROR"


class ContentConflict(PipelineError):
    """An immutable path already contains different bytes."""

    code = "CONTENT_CONFLICT"


class BudgetExceeded(PipelineError):
    """A bounded run has exhausted its request, response, or time budget."""

    code = "BUDGET_EXCEEDED"


class DeferredError(PipelineError):
    """Retry-After requests a delay longer than this run can safely wait."""

    code = "RETRY_DEFERRED"

    def __init__(self, message, retry_after_seconds=0):
        """Expose only the upstream delay needed by a future scheduled tick."""
        self.retry_after_seconds = retry_after_seconds
        super().__init__(message)


class HTTPFailure(PipelineError):
    """An external service returned a terminal or exhausted HTTP failure."""

    code = "HTTP_FAILURE"

    def __init__(self, service: str, status: int | None) -> None:
        """Keep only service and status, never raw upstream exception text."""
        self.status = status
        super().__init__(f"{service}: HTTP {status}" if status else f"{service}: transport failed")


class TransportFailure(Exception):
    """Internal transport signal; its underlying network exception is not logged."""

    def __init__(self, retryable: bool = True) -> None:
        """Classify certificate failures separately from temporary connectivity loss."""
        self.retryable = retryable
        super().__init__("transport unavailable")
