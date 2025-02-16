"""Stream type classes for tap-amazonads."""

from __future__ import annotations

import typing as t
from pathlib import Path
from singer_sdk import typing as th
import requests

from tap_amazonads.client import AmazonADsStream

SCHEMAS_DIR = Path(__file__).parent / "schemas"

class CampaignsStream(AmazonADsStream):
    """Campaigns stream."""
    
    name = "campaigns"
    path = "/sp/campaigns/list"  # Reverting back to original path
    primary_keys: t.ClassVar[list[str]] = ["campaignId"]
    replication_key = None  # Removing replication key since lastUpdatedDateTime is not available
    schema_filepath = SCHEMAS_DIR / "campaigns.json"
    method = "POST"  # Default to POST for SP and SB
    page_size = 100  # Default page size
    records_jsonpath = "$.campaigns[*]"  # Updated to match actual response structure
    
    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {
            "Content-Type": "application/vnd.spcampaign.v3+json",
            "Accept": "application/vnd.spcampaign.v3+json",
        }
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_url_params(self, context: dict | None, next_page_token: t.Any | None) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if context and "adProduct" in context:
            params["adProduct"] = context["adProduct"]
        return params

    def get_starting_timestamp(self, context: dict | None) -> str:
        """Return the starting timestamp for incremental sync."""
        from datetime import datetime
        start_date = self.get_starting_replication_key_value(context)
        if start_date:
            # If it's already a string in ISO format, return it
            if isinstance(start_date, str):
                return start_date
            # If it's a datetime, convert to ISO format
            if isinstance(start_date, datetime):
                return start_date.isoformat()
        # Default to config start_date or a fixed date much earlier
        return self.config.get("start_date", "2023-01-01T00:00:00Z")

    def get_ending_timestamp(self, context: dict | None) -> str | None:
        """Return the ending timestamp for incremental sync."""
        # For initial sync, don't set an end date to get all records
        if not self.get_starting_replication_key_value(context):
            return None
        # For subsequent syncs, use current time as end date
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).isoformat()

    def get_request_body(self, context: dict | None, next_page_token: t.Any | None) -> dict | None:
        """Return a dictionary to be sent in the request body."""
        if self.method == "GET":
            return None
        
        # For Sponsored Products - include pagination, adProduct, date filtering, and state
        return {
            "startIndex": int(next_page_token) if next_page_token else 0,
            "count": self.page_size,
            "adProduct": "SPONSORED_PRODUCTS",  # Required field
            "startDateFilter": {
                "startDate": "2023-01-01",  # Much earlier start date
                "endDate": "2024-12-31"     # Future end date
            },
            "state": "ENABLED"  # Try with just ENABLED campaigns first
        }

    def prepare_request(self, context: dict | None, next_page_token: t.Any | None) -> requests.PreparedRequest:
        """Prepare a request object for the REST API."""
        http_method = self.method
        url: str = self.get_url(context)
        params: dict = self.get_url_params(context, next_page_token)
        request_data = self.get_request_body(context, next_page_token)
        headers = self.http_headers

        return self.build_prepared_request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            json=request_data,
        )

    def get_path(self, context: dict | None) -> str:
        """Return the API endpoint path."""
        ad_product = context.get("adProduct", "SPONSORED_PRODUCTS").lower() if context else "sponsored_products"
        if ad_product == "sponsored_products":
            self.method = "POST"
            return "/sp/campaigns/list"
        elif ad_product == "sponsored_brands":
            self.method = "POST"
            return "/sb/v4/campaigns/list"
        elif ad_product == "sponsored_display":
            self.method = "GET"
            return "/sd/campaigns"
        return self.path

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "campaignId": record["campaignId"],
            "adProduct": record.get("adProduct", "SPONSORED_PRODUCTS")
        }


class AdGroupsStream(AmazonADsStream):
    """Ad Groups stream."""
    
    name = "ad_groups"
    path = "/sp/adGroups/list"
    primary_keys = ["adGroupId"]
    replication_key = None  # Removing replication key since lastUpdatedDateTime is not available
    records_jsonpath = "$.adGroups[*]"
    parent_stream_type = CampaignsStream
    method = "POST"  # Default to POST for SP and SB
    records_jsonpath = "$.adGroups[*]"  # Updated to match actual response structure
    ignore_parent_replication_keys = True
    schema_filepath = SCHEMAS_DIR / "ad_groups.json"
    
    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {
            "Content-Type": "application/vnd.spadGroup.v3+json",
            "Accept": "application/vnd.spadGroup.v3+json",
        }
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_url_params(self, context: dict | None, next_page_token: t.Any | None) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if context and "adProduct" in context:
            params["adProduct"] = context["adProduct"]
        return params

    def get_request_body(self, context: dict | None, next_page_token: t.Any | None) -> dict | None:
        """Return a dictionary to be sent in the request body."""
        request_data = {
            "startIndex": int(next_page_token) if next_page_token else 0,
            "count": self.page_size,
            "adProduct": "SPONSORED_PRODUCTS",  # Required field
            "state": "enabled,paused,archived"  # Changed from stateFilter to state
        }
        if context:
            request_data["campaignId"] = context["campaignId"]
        return request_data

    def prepare_request(self, context: dict | None, next_page_token: t.Any | None) -> requests.PreparedRequest:
        """Prepare a request object for the REST API."""
        http_method = self.method
        url: str = self.get_url(context)
        params: dict = self.get_url_params(context, next_page_token)
        request_data = self.get_request_body(context, next_page_token)
        headers = self.http_headers

        return self.build_prepared_request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            json=request_data,
        )

    def get_path(self, context: dict | None) -> str:
        """Return the API endpoint path."""
        ad_product = context.get("adProduct", "SPONSORED_PRODUCTS").lower() if context else "sponsored_products"
        if ad_product == "sponsored_products":
            self.method = "POST"
            return "/sp/adGroups/list"
        elif ad_product == "sponsored_brands":
            self.method = "POST"
            return "/sb/v4/adGroups/list"
        elif ad_product == "sponsored_display":
            self.method = "GET"
            return "/sd/adGroups"
        return self.path

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "adGroupId": record["adGroupId"],
            "campaignId": record["campaignId"],
            "adProduct": context.get("adProduct", "SPONSORED_PRODUCTS") if context else record.get("adProduct", "SPONSORED_PRODUCTS")
        }


class TargetsStream(AmazonADsStream):
    """Targets stream."""
    
    name = "targets"
    path = "/sp/targets/list"  # Default to Sponsored Products
    primary_keys: t.ClassVar[list[str]] = ["targetId"]
    replication_key = "lastUpdatedDateTime"
    schema_filepath = SCHEMAS_DIR / "targets.json"
    parent_stream_type = AdGroupsStream
    method = "POST"  # Default to POST for SP and SB
    records_jsonpath = "$.targetingClauses[*]"
    ignore_parent_replication_keys = True
    
    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {
            "Content-Type": "application/vnd.sptargetingClause.v3+json",
            "Accept": "application/vnd.sptargetingClause.v3+json",
        }
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_url_params(self, context: dict | None, next_page_token: t.Any | None) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if context and "adProduct" in context:
            params["adProduct"] = context["adProduct"]
        return params

    def get_request_body(self, context: dict | None, next_page_token: t.Any | None) -> dict | None:
        """Return a dictionary to be sent in the request body."""
        request_data = {
            "startIndex": int(next_page_token) if next_page_token else 0,
            "count": self.page_size,
            "adProduct": "SPONSORED_PRODUCTS",  # Required field
            "state": "enabled,paused,archived"  # Changed from stateFilter to state
        }
        if context:
            request_data["adGroupId"] = context["adGroupId"]
        return request_data

    def prepare_request(self, context: dict | None, next_page_token: t.Any | None) -> requests.PreparedRequest:
        """Prepare a request object for the REST API."""
        http_method = self.method
        url: str = self.get_url(context)
        params: dict = self.get_url_params(context, next_page_token)
        request_data = self.get_request_body(context, next_page_token)
        headers = self.http_headers

        return self.build_prepared_request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            json=request_data,
        )

    def get_path(self, context: dict | None) -> str:
        """Return the API endpoint path."""
        ad_product = context.get("adProduct", "SPONSORED_PRODUCTS").lower() if context else "sponsored_products"
        if ad_product == "sponsored_products":
            self.method = "POST"
            return "/sp/targets/list"
        elif ad_product == "sponsored_brands":
            self.method = "POST"
            return "/sb/targets/list"
        elif ad_product == "sponsored_display":
            self.method = "GET"
            return "/sd/targets"
        return self.path


class AdsStream(AmazonADsStream):
    """Ads stream."""
    
    name = "ads"
    path = "/sp/productAds/list"
    primary_keys = ["adId"]
    replication_key = None  # Removing replication key since lastUpdatedDateTime is not available
    records_jsonpath = "$.productAds[*]"
    parent_stream_type = AdGroupsStream
    ignore_parent_replication_keys = True
    schema_filepath = SCHEMAS_DIR / "ads.json"
    method = "POST"

    headers = {
        "Content-Type": "application/vnd.spproductAd.v3+json",
        "Accept": "application/vnd.spproductAd.v3+json",
    }

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {
            "Content-Type": "application/vnd.spproductAd.v3+json",
            "Accept": "application/vnd.spproductAd.v3+json",
        }
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_url_params(self, context: dict | None, next_page_token: t.Any | None) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if context and "adProduct" in context:
            params["adProduct"] = context["adProduct"]
        return params

    def get_request_body(self, context: dict | None, next_page_token: t.Any | None) -> dict | None:
        """Return a dictionary to be sent in the request body."""
        request_data = {
            "startIndex": int(next_page_token) if next_page_token else 0,
            "count": self.page_size,
            "adProduct": "SPONSORED_PRODUCTS",  # Required field
            "state": "enabled,paused,archived"  # Changed from stateFilter to state
        }
        if context:
            request_data["adGroupId"] = context["adGroupId"]
        return request_data

    def prepare_request(self, context: dict | None, next_page_token: t.Any | None) -> requests.PreparedRequest:
        """Prepare a request object for the REST API."""
        http_method = self.method
        url: str = self.get_url(context)
        params: dict = self.get_url_params(context, next_page_token)
        request_data = self.get_request_body(context, next_page_token)
        headers = self.http_headers

        return self.build_prepared_request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            json=request_data,
        )

    def get_path(self, context: dict | None) -> str:
        """Return the API endpoint path."""
        ad_product = context.get("adProduct", "SPONSORED_PRODUCTS").lower() if context else "sponsored_products"
        if ad_product == "sponsored_products":
            self.method = "POST"
            return "/sp/productAds/list"
        elif ad_product == "sponsored_brands":
            self.method = "POST"
            return "/sb/v4/ads/list"
        elif ad_product == "sponsored_display":
            self.method = "GET"
            return "/sd/productAds"
        return self.path
