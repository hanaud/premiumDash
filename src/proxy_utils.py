"""
Utility functions for handling proxy URLs with special characters.

Proxy URLs often contain credentials that may have special characters requiring
URL encoding for proper transmission through HTTP libraries.
"""

from urllib.parse import quote
import logging

logger = logging.getLogger(__name__)


def encode_proxy_url(proxy_url: str) -> str:
    """
    Properly encode special characters in proxy URLs.

    Proxy URLs often contain credentials with special characters that need
    URL encoding. For example:
    - Input:  http://user:pass!@#$%@proxy.com:3128
    - Output: http://user:pass%21%40%23%24%25@proxy.com:3128

    Special characters that commonly appear in passwords:
    - ! (0x21) → %21
    - @ (0x40) → %40
    - # (0x23) → %23
    - $ (0x24) → %24
    - % (0x25) → %25
    - * (0x2A) → %2A
    - & (0x26) → %26
    - : (0x3A) → %3A (only in password, not between host:port)

    Args:
        proxy_url: Raw proxy URL that may contain special characters

    Returns:
        Proxy URL with credentials properly percent-encoded

    Examples:
        >>> encode_proxy_url("http://user:pass123@proxy.com:3128")
        'http://user:pass123@proxy.com:3128'

        >>> encode_proxy_url("http://user:p@ss!@proxy.com:3128")
        'http://user:p%40ss%21@proxy.com:3128'

        >>> encode_proxy_url("socks5://user:pass*@proxy.com:1080")
        'socks5://user:pass%2A@proxy.com:1080'
    """
    if not proxy_url:
        return proxy_url

    try:
        # Parse the URL into components
        if "://" not in proxy_url:
            return proxy_url

        protocol, rest = proxy_url.split("://", 1)

        # Check if there are credentials (user:pass@host:port)
        if "@" not in rest:
            return proxy_url  # No credentials, return as-is

        credentials, hostport = rest.rsplit("@", 1)

        # Split credentials into user and password
        if ":" in credentials:
            user, password = credentials.split(":", 1)
            # Encode special characters in password
            # Safe characters: alphanumeric + hyphen + underscore + period + tilde
            encoded_password = quote(password, safe='')
            encoded_creds = f"{user}:{encoded_password}"
        else:
            # No password, just encode username
            encoded_creds = quote(credentials, safe='')

        # Reconstruct the proxy URL
        return f"{protocol}://{encoded_creds}@{hostport}"

    except Exception as e:
        logger.warning(f"Failed to encode proxy URL: {e}; using original")
        return proxy_url
