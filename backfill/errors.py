"""Exception taxonomy. The retry policy depends on it:

* PermanentError  -> never retried (retrying cannot help: bad schema, missing file ...)
* anything else   -> treated as transient and retried with exponential backoff
"""


class PermanentError(Exception):
    """Failure that a retry cannot fix."""


class SchemaError(PermanentError):
    """Source file layout does not match the contract (missing required column ...)."""


class SourceMissingError(PermanentError):
    """The partition (or a dimension) file does not exist."""


class TransientError(Exception):
    """Failure that is expected to succeed on retry (lock, I/O hiccup, simulated fault)."""
