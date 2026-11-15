"""Errors of host calls and handlers; uncaught, each one ends the run as the table in the docs says."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0


class ArunaError(Exception):
    """Every error the library raises."""

    #: How an uncaught error ends the run: "permanent", "retryable" or "invalid-input".
    outcome = "permanent"


class NotAllowed(ArunaError):
    """The install may not read or write what the call names."""


class NotFound(ArunaError):
    pass


class Conflict(ArunaError):
    """Something changed since it was read."""


class LimitReached(ArunaError):
    pass


class InvalidCall(ArunaError):
    """The call's arguments are not valid."""


class Unavailable(ArunaError):
    """The host cannot answer now; the run is tried again later."""

    outcome = "retryable"


class UnknownOutcome(ArunaError):
    """A change may have happened without its answer; a person decides."""


class ServiceError(ArunaError):
    """A service answered with a status outside 200 to 299."""

    def __init__(self, status, message):
        super().__init__(f"the service answered {status}: {message}")
        self.status = status


class Fail(ArunaError):
    """The plugin gives up; the run fails."""


class Retry(ArunaError):
    """The plugin asks for the run to be tried again later."""

    outcome = "retryable"


class InvalidInput(ArunaError):
    """The run's input does not fit the plugin."""

    outcome = "invalid-input"


#: The error each host error kind raises.
BY_KIND = {
    "not-granted": NotAllowed,
    "denied": NotAllowed,
    "not-found": NotFound,
    "conflict": Conflict,
    "limit": LimitReached,
    "invalid": InvalidCall,
    "unavailable": Unavailable,
    "unknown-outcome": UnknownOutcome,
}
