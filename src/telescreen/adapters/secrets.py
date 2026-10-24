from __future__ import annotations

import os
from collections.abc import Mapping

from telescreen.application.ports import FatalPipelineError


class SecretNotFoundError(FatalPipelineError, LookupError):
    pass


class EnvSecretResolver:
    """Resolves ``env:NAME`` references from environment variables."""

    def __init__(self, environ: Mapping[str, str] | None = None) -> None:
        self._environ = os.environ if environ is None else environ

    def resolve(self, reference: str) -> str:
        scheme, sep, name = reference.partition(":")
        if scheme != "env" or not sep or not name:
            raise SecretNotFoundError(f"Unsupported secret reference: {reference!r}")
        try:
            return self._environ[name]
        except KeyError:
            raise SecretNotFoundError(f"Environment variable {name} is not set") from None
