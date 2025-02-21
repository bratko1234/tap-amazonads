"""AmazonADs Authentication."""

from __future__ import annotations

from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta
from typing import Any
import logging
import requests
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


# The SingletonMeta metaclass makes your streams reuse the same authenticator instance.
# If this behaviour interferes with your use-case, you can remove the metaclass.
class AmazonADsAuthenticator:
    """Authenticator for Amazon Ads."""

    def __init__(self, config):
        """Initialize authenticator."""
        self._config = config
        self._access_token = None
        self._token_expiry = None
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("Starting AmazonADsAuthenticator initialization")
        self.logger.info(f"Config keys available: {list(config.keys())}")
        
        # Required config keys
        required_keys = [
            'refresh_token',
            'client_id',
            'client_secret',
            'profile_id'
        ]
        
        # Validate required config
        for key in required_keys:
            if key not in config:
                raise Exception(f"Missing required config key: {key}")
        
        self.refresh_access_token()
        self.logger.info("AmazonADsAuthenticator initialized")

    def refresh_access_token(self):
        """Refresh access token."""
        url = "https://api.amazon.com/auth/o2/token"
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self._config["refresh_token"],
            "client_id": self._config["client_id"],
            "client_secret": self._config["client_secret"],
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"
        }
        
        response = requests.post(url, data=data, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Failed to refresh token: {response.text}")
        
        response_json = response.json()
        self._access_token = response_json["access_token"]
        self._token_expiry = datetime.now() + timedelta(seconds=response_json["expires_in"] - 300)  # 5 min buffer
        
    @classmethod
    def create_for_stream(cls, stream) -> "AmazonADsAuthenticator":
        """Create a new authenticator for the given stream."""
        logger.info(f"Creating authenticator for stream: {stream.name}")
        logger.info(f"Stream config: {stream.config}")
        logger.info(f"Stream tap config: {stream.tap.config}")
        
        auth = cls(
            stream.config.get("auth_endpoint"),
            stream.config
        )
        return auth

    def get_auth_headers(self):
        """Get the authentication headers."""
        if not self._access_token or (self._token_expiry and datetime.now() >= self._token_expiry):
            self.refresh_access_token()
            
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Amazon-Advertising-API-ClientId": self._config["client_id"],
            "Content-Type": "application/json"
        }

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the Amazon Ads API.

        Returns:
            A dict with the request body
        """
        logger.info("Building OAuth request body")
        body = {
            "grant_type": "refresh_token",
            "refresh_token": self._config["refresh_token"],
            "client_id": self._config["client_id"],
            "client_secret": self._config["client_secret"],
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
            "Amazon-Advertising-API-ClientId": self._config["client_id"],
            "Amazon-Advertising-API-Scope": self._config["profile_id"],
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