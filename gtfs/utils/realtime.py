import requests
from google.transit import gtfs_realtime_pb2 as gtfs_rt


def databus():
    print("I am very glad to inform you that the gtfs-django databus integration for development has worked as expected :)")

def databus2():
    print("Yet another proof of the integration working properly; now we can really say it's working. Let's gooo!")

def gtfs_realtime_import(url: str) -> dict:
    """Fetch and parse a GTFS Realtime feed from a URL.

    Args:
        url (str): The URL of the GTFS Realtime feed.

    Returns:
        dict: A parsed `FeedMessage` object.

    Raises:
        requests.RequestException: If the HTTP request fails.
        google.protobuf.message.DecodeError: If parsing the feed fails.

    Examples:
        >>> feed = gtfs_realtime_import("https://example.com/gtfs-realtime")
        >>> feed.header.gtfs_realtime_version
        '2.0'
    """
    feed_message_pb = requests.get(url).content
    feed_message = gtfs_rt.FeedMessage()
    feed_message.ParseFromString(feed_message_pb)
    return feed_message


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
