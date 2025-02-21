"""AmazonADs Authentication."""

from __future__ import annotations

from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta
from typing import Any
import logging

logger = logging.getLogger(__name__)


# The SingletonMeta metaclass makes your streams reuse the same authenticator instance.
# If this behaviour interferes with your use-case, you can remove the metaclass.
class AmazonADsAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for Amazon Ads API."""

    def __init__(self, *args, **kwargs):
        logger.info("Starting AmazonADsAuthenticator initialization")
        super().__init__(*args, **kwargs)
        logger.info("AmazonADsAuthenticator initialized")
        logger.info(f"Config keys available: {list(self.config.keys())}")

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the Amazon Ads API.

        Returns:
            A dict with the request body
        """
        logger.info("Building OAuth request body")
        body = {
            "grant_type": "refresh_token",
            "refresh_token": self.config["refresh_token"],
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
        }
        logger.info("OAuth request body built successfully")
        return body

    def update_access_token(self) -> None:
        """Update `access_token` using refresh token."""
        logger.info("Starting token refresh process")
        try:
            token_response = self._make_oauth_request()
            logger.info("Token refresh request completed")
            self.access_token = token_response.json()["access_token"]
            logger.info("Access token updated successfully")
        except Exception as e:
            logger.error(f"Token refresh failed: {str(e)}")
            logger.error(f"Response content: {token_response.text if 'token_response' in locals() else 'No response'}")
            raise

    @classmethod
    def create_for_stream(cls, stream) -> AmazonADsAuthenticator:
        """Create a new authenticator for the Amazon Ads API.

        Args:
            stream: The Singer stream instance.

        Returns:
            A new authenticator instance.
        """
        logger.info("Creating new authenticator for stream")
        auth = cls(
            stream=stream,
            auth_endpoint="https://api.amazon.com/auth/o2/token",
            oauth_scopes="advertising::campaign_management"
        )
        logger.info("New authenticator created successfully")
        return auth

    def get_auth_params(self, context: dict | None = None) -> dict[str, Any]:
        """Get auth headers for the Amazon Ads API.

        Args:
            context: Optional stream context.

        Returns:
            Auth headers dict.
        """
        logger.info("Getting auth params")
        if not self.access_token:
            logger.warning("No access token available, attempting to refresh")
            self.update_access_token()
        
        auth_params = {
            "Amazon-Advertising-API-ClientId": self.config["client_id"],
            "Amazon-Advertising-API-Scope": self.config["profile_id"],
            "Authorization": f"Bearer {self.access_token}"
        }
        logger.info("Auth params generated successfully")
        return auth_params

    def get_auth_headers(self, context: dict | None = None) -> dict[str, Any]:
        """Get auth headers for the Amazon Ads API.

        Args:
            context: Optional stream context.

        Returns:
            Auth headers dict.
        """
        return self.get_auth_params(context)