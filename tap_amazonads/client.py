"""REST client handling, including AmazonADsStream base class."""

from __future__ import annotations

import decimal
import typing as t
from functools import cached_property
from pathlib import Path
import backoff
import requests
from requests import Response
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
import gzip
import json

from tap_amazonads.auth import AmazonADsAuthenticator

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Auth, Context

SCHEMAS_DIR = Path(__file__).parent / "schemas"

# Define custom error classes for Amazon Ads API
class AmazonAdsError(Exception):
    """Base exception for Amazon Ads API errors."""

class AmazonAdsRetriableError(AmazonAdsError):
    """Retriable error from Amazon Ads API."""

class AmazonAdsFatalError(AmazonAdsError):
    """Fatal error from Amazon Ads API."""


class AmazonAdsPaginator(BaseAPIPaginator):
    """Paginator for Amazon Ads API pagination."""

    def __init__(
        self,
        start_value: int = 0,
        page_size: int = 100,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> None:
        """Initialize the paginator.

        Args:
            start_value: The starting index.
            page_size: The page size.
            args: Additional positional arguments.
            kwargs: Additional keyword arguments.
        """
        super().__init__(start_value, *args, **kwargs)
        self._page_size = page_size
        self._value = start_value

    def get_next(self, response: Response) -> int | None:
        """Get the next page token.

        Args:
            response: API response object.

        Returns:
            The next page index, or None if no more pages.
        """
        data = response.json()
        if not data.get("pagination"):
            return None
        
        total_results = data["pagination"].get("totalResults", 0)
        if self._value + self._page_size >= total_results:
            return None
        
        self._value += self._page_size
        return self._value


class AmazonADsStream(RESTStream):
    """AmazonADs stream class."""

    # Common settings for all streams
    records_jsonpath = "$.data[*]"  # Amazon Ads API typically returns data in a 'data' field
    next_page_token_jsonpath = None  # We'll use our custom paginator
    page_size = 100
    selected_properties: set[str] = set()  # Set of selected property paths

    def get_selected_properties(self) -> set[str]:
        """Get set of selected property names."""
        if not self.selected:
            return set()
        
        if not self.selected_properties:
            return super().get_selected_properties()
            
        return self.selected_properties
    
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        region = self.config.get("region", "NA")  # Default to North America
        if region == "EU":
            return "https://advertising-api-eu.amazon.com"
        elif region == "FE":
            return "https://advertising-api-fe.amazon.com"
        return "https://advertising-api.amazon.com"  # NA region

    @cached_property
    def authenticator(self) -> Auth:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return AmazonADsAuthenticator.create_for_stream(self)

    def get_new_paginator(self) -> AmazonAdsPaginator:
        """Create a new pagination helper instance.

        Returns:
            A pagination helper instance.
        """
        return AmazonAdsPaginator(page_size=self.page_size)

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response.

        Args:
            response: A requests.Response object.

        Raises:
            FatalAPIError: If the response contains a fatal error.
            RetriableAPIError: If the response contains a retriable error.
        """
        if response.status_code == 429:
            msg = f"Rate limit exceeded: {response.text}"
            raise RetriableAPIError(msg)
        
        if response.status_code == 401:
            msg = f"Authorization failed: {response.text}"
            raise FatalAPIError(msg)
        
        if response.status_code >= 400 and response.status_code < 500:
            msg = f"Client error: {response.text}"
            raise FatalAPIError(msg)
        
        if response.status_code >= 500:
            msg = f"Server error: {response.text}"
            raise RetriableAPIError(msg)

    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.RequestException),
        max_tries=7,
        factor=3,
    )
    def _request(
        self,
        prepared_request: requests.PreparedRequest,
        context: dict | None = None,
    ) -> requests.Response:
        """Perform HTTP request with backoff and error handling.

        Args:
            prepared_request: The prepared request to send.
            context: Stream context.

        Returns:
            Response from the API.
        """
        # Add authentication headers
        auth_headers = self.authenticator.get_auth_headers()
        prepared_request.headers.update(auth_headers)
        
        response = super()._request(prepared_request, context)
        
        # Log the full request and response for debugging
        self.logger.info(f"Request URL: {prepared_request.url}")
        self.logger.info(f"Request Method: {prepared_request.method}")
        self.logger.info(f"Request Headers: {prepared_request.headers}")
        self.logger.info(f"Request Body: {prepared_request.body}")
        self.logger.info(f"Response Status: {response.status_code}")
        self.logger.info(f"Response Headers: {response.headers}")
        self.logger.info(f"Response Body: {response.text}")
        
        return response

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        try:
            if response.headers.get("Content-Encoding") == "gzip":
                data = json.loads(gzip.decompress(response.content).decode())
            else:
                data = response.json(parse_float=decimal.Decimal)
        except Exception as e:
            msg = f"Failed to parse response: {str(e)}"
            raise FatalAPIError(msg) from e

        yield from extract_jsonpath(self.records_jsonpath, data)

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Filter row to include only selected properties."""
        if not self.selected_properties:
            return row
        
        filtered_row = {}
        for prop in self.selected_properties:
            if "." in prop:
                # Handle nested properties
                parts = prop.split(".")
                value = row
                for part in parts:
                    value = value.get(part, {})
                if value != {}:
                    filtered_row[parts[-1]] = value
            else:
                # Handle top-level properties
                if prop in row:
                    filtered_row[prop] = row[prop]
        return filtered_row
