"""AmazonADs Authentication."""

from __future__ import annotations

from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta
from typing import Any
import logging
import requests
from datetime import datetime, timedelta, timezone

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
        self.logger = logging.getLogger(__name__)  # Prvo inicijaliziramo logger
        
        self.logger.info("=== Starting AmazonADsAuthenticator initialization ===")
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
                self.logger.error(f"Missing required config key: {key}")
                raise Exception(f"Missing required config key: {key}")
        
        self.refresh_access_token()
        self.logger.info(f"After refresh, access token (first 20 chars): {self._access_token[:20] if self._access_token else 'None'}")
        self.logger.info("=== AmazonADsAuthenticator initialization complete ===")

    @property
    def access_token(self) -> str:
        """Return the current access token."""
        self.logger.info(f"Access token requested, current value (first 20 chars): {self._access_token[:20] if self._access_token else 'None'}")
        return self._access_token

    def refresh_access_token(self):
        """Refresh the access token using the refresh token."""
        url = "https://api.amazon.com/auth/o2/token"
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self._config["refresh_token"],
            "client_id": self._config["client_id"],
            "client_secret": self._config["client_secret"]
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        logger.info("Refreshing access token...")
        response = requests.post(url, data=data, headers=headers)
        
        if response.status_code != 200:
            raise Exception(f"Failed to refresh token: {response.text}")
        
        token_data = response.json()
        self._access_token = token_data["access_token"]
        self._token_expiry = datetime.now(timezone.utc) + timedelta(seconds=token_data["expires_in"])
        
        logger.info(f"Successfully refreshed access token. Expires in {token_data['expires_in']} seconds")
        return self._access_token

    @classmethod
    def create_for_stream(cls, stream):
        """Create a new authenticator for the given stream.
        
        Args:
            stream: The stream instance requiring authentication
            
        Returns:
            A new authenticator instance
        """
        logger.info("=== Creating new authenticator for stream ===")
        logger.info(f"Stream type: {type(stream)}")
        logger.info(f"Stream config: {stream.config}")
        auth = cls(stream.config)
        logger.info(f"New authenticator created with access token (first 20 chars): {auth.access_token[:20] if auth.access_token else 'None'}")
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

class AmazonADsNonReportAuthenticator:
    """Authenticator for non-report Amazon Ads endpoints."""

    def __init__(self, config):
        """Initialize authenticator."""
        self._config = config
        self._access_token = None
        self._token_expires_at = None
        
    def __call__(self, request):
        """Called by requests library to authenticate requests."""
        auth_headers = self.get_auth_headers()
        request.headers.update(auth_headers)
        return request

    @property
    def access_token(self):
        """Get the current access token."""
        if not self._access_token or self._token_expired:
            self.update_access_token()
        return self._access_token

    def update_access_token(self):
        """Update the access token using the refresh token."""
        token_response = requests.post(
            "https://api.amazon.com/auth/o2/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": self._config["refresh_token"],
                "client_id": self._config["client_id"],
                "client_secret": self._config["client_secret"],
            },
        )
        token_response.raise_for_status()
        self._access_token = token_response.json()["access_token"]
        logger.info(f"After refresh, access token (first 20 chars): {self._access_token[:20]}")

    def get_auth_headers(self, context: dict | None = None) -> dict[str, Any]:
        """Get auth headers for the Amazon Ads API."""
        return {
            "Amazon-Advertising-API-ClientId": self._config["client_id"],
            "Amazon-Advertising-API-Scope": self._config["profile_id"],
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/vnd.sptargetingClause.v3+json",
            "Accept": "application/vnd.sptargetingClause.v3+json"
        }
