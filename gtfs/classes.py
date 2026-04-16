class RealtimeClient:
    """Client for fetching GTFS Realtime status.

    Attributes:
        base_url (str): Base URL for the realtime endpoint.
        timeout (float): Request timeout in seconds.
        headers (dict[str, str] | None): Optional request headers.

    Args:
        base_url (str): Base URL for the realtime endpoint.
        timeout (float, optional): Request timeout in seconds.
        headers (dict[str, str] | None, optional): Optional request headers.
    """

    def __init__(
        self,
        base_url: str,
        timeout: float = 10.0,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.base_url = base_url
        self.timeout = timeout
        self.headers = headers

    def get_status(self, include_alerts: bool = False) -> dict[str, str]:
        """Return a placeholder status payload.

        Args:
            include_alerts (bool, optional): Whether to include service alerts.

        Returns:
            dict[str, str]: A placeholder status payload.

        Raises:
            NotImplementedError: Always raised because this is a stub.

        Examples:
            >>> client = RealtimeClient("https://example.com/gtfs-realtime")
            >>> client.get_status(include_alerts=True)
            {'status': 'not_implemented'}
        """
        raise NotImplementedError("Example stub for documentation tests.")
