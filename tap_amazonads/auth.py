"""AmazonADs Authentication."""

from __future__ import annotations

from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta
from typing import Any


# The SingletonMeta metaclass makes your streams reuse the same authenticator instance.
# If this behaviour interferes with your use-case, you can remove the metaclass.
class AmazonADsAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for Amazon Ads API."""

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the Amazon Ads API.

        Returns:
            A dict with the request body
        """
        return {
            "grant_type": "refresh_token",
            "refresh_token": self.config["refresh_token"],
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
        }

    @classmethod
    def create_for_stream(cls, stream) -> AmazonADsAuthenticator:
        """Create a new authenticator for the Amazon Ads API.

        Args:
            stream: The Singer stream instance.

        Returns:
            A new authenticator instance.
        """
        return cls(
            stream=stream,
            auth_endpoint="https://api.amazon.co.jp/auth/o2/token",
            oauth_scopes="advertising::campaign_management"
        )

    def get_auth_params(self, context: dict | None = None) -> dict[str, Any]:
        """Get auth headers for the Amazon Ads API.

        Args:
            context: Optional stream context.

        Returns:
            Auth headers dict.
        """
        return {
            "Amazon-Advertising-API-ClientId": self.config["client_id"],
            "Amazon-Advertising-API-Scope": self.config["profile_id"],
            "Authorization": f"Bearer {self.access_token}"
        }

    def get_auth_headers(self, context: dict | None = None) -> dict[str, Any]:
        """Get auth headers for the Amazon Ads API.

        Args:
            context: Optional stream context.

        Returns:
            Auth headers dict.
        """
        return self.get_auth_params(context)
