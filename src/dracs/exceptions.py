class DracsError(Exception):
    """Base exception for DRACS application errors."""

    pass


class ValidationError(DracsError):
    """Raised when input validation fails."""

    pass


class DatabaseError(DracsError):
    """Raised when database operations fail."""

    pass


class APIError(DracsError):
    """Raised when API calls fail."""

    pass


class SNMPError(DracsError):
    """Raised when SNMP operations fail."""

    pass
