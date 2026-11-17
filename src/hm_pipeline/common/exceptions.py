class PipelineError(Exception):
    """Base exception for expected pipeline failures."""


class ConfigurationError(PipelineError):
    """Raised for invalid or incomplete configuration."""


class ValidationError(PipelineError):
    """Raised when a source row does not satisfy the expected schema."""


class DeliveryError(PipelineError):
    """Raised when Kafka does not acknowledge a produced batch."""


class StorageError(PipelineError):
    """Raised when a raw source upload cannot be verified."""
