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
class AmazonADsAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for Amazon Ads API."""

    def __init__(self, *args, **kwargs):
        """Initialize authenticator."""
        self._config = kwargs
        self._access_token = None
        self._token_expiry = None
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("Starting AmazonADsAuthenticator initialization")
        self.logger.info(f"Config keys available: {list(kwargs.keys())}")
        
        # Validate required config
        required_keys = ['refresh_token', 'client_id', 'client_secret']
        for key in required_keys:
            if key not in kwargs:
                raise Exception(f"Missing required config key: {key}")
        
        # Initialize with a fresh token
        self.refresh_access_token()
        self.logger.info("AmazonADsAuthenticator initialized")

    def refresh_access_token(self):
        """Refresh the access token using the refresh token."""
        token_url = 'https://api.amazon.com/auth/o2/token'
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': self._config['refresh_token'],
            'client_id': self._config['client_id'],
            'client_secret': self._config['client_secret']
        }
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'
        }
        
        try:
            response = requests.post(token_url, data=data, headers=headers)
            response.raise_for_status()
            token_data = response.json()
            
            self._access_token = token_data['access_token']
            self._token_expiry = datetime.now() + timedelta(seconds=token_data['expires_in'] - 300)  # Buffer of 5 minutes
            
            self.logger.info("Successfully refreshed access token")
        except Exception as e:
            self.logger.error(f"Failed to refresh access token: {str(e)}")
            raise

    def get_access_token(self):
        """Get the current access token, refreshing if necessary."""
        if not self._access_token or (self._token_expiry and datetime.now() >= self._token_expiry):
            self.refresh_access_token()
        return self._access_token

    def get_auth_headers(self):
        """Get the authentication headers."""
        return {
            "Authorization": f"Bearer {self.get_access_token()}",
            "Amazon-Advertising-API-ClientId": self._config['client_id'],
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