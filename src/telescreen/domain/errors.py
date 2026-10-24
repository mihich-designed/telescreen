class DomainError(Exception):
    """Base class for violations of domain rules."""


class InvalidPipelineError(DomainError):
    pass
