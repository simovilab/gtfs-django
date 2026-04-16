import requests


def example_realtime_status(
    url: str,
    timeout: float = 10.0,
    headers: dict[str, str] | None = None,
    include_alerts: bool = False,
) -> dict[str, str]:
    """Returns a placeholder status for a GTFS Realtime endpoint.

    This function exists only to validate documentation generation.

    Args:
        url (str): The endpoint to check.
        timeout (float): Request timeout in seconds.
        headers (dict[str, str] | None): Optional request headers.
        include_alerts (bool): Whether to include service alerts.

    Returns:
        dict[str, str]: A placeholder status payload.

    Raises:
        NotImplementedError: Always raised because this is a documentation stub.

    warning: Important
        This function is intentionally non-functional and should be replaced
        with a real implementation when needed.

    Examples:
        >>> example_realtime_status("https://example.com/gtfs-realtime")
        {'status': 'not_implemented'}
        >>> example_realtime_status(
        ...     "https://example.com/gtfs-realtime",
        ...     timeout=2.5,
        ...     headers={"Authorization": "Bearer demo"},
        ...     include_alerts=True,
        ... )
        {'status': 'not_implemented'}
    """
    raise NotImplementedError("Example stub for documentation tests.")
