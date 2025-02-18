"""Stream type classes for tap-amazonads."""

from __future__ import annotations

import typing as t
from pathlib import Path
from singer_sdk import typing as th
import requests
import logging

from tap_amazonads.client import AmazonADsStream

SCHEMAS_DIR = Path(__file__).parent / "schemas"

logger = logging.getLogger(__name__)

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


class AdGroupsStream(AmazonADsStream):
    """Ad Groups stream."""
    
    name = "ad_groups"
    path = "/sp/adGroups/list"
    primary_keys = ["adGroupId"]
    replication_key = None  # Removing replication key since lastUpdatedDateTime is not available
    records_jsonpath = "$.adGroups[*]"
   # parent_stream_type = CampaignsStream
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


class TargetsStream(AmazonADsStream):
    """Targets stream."""
    
    name = "targets"
    path = "/sp/targets/list"  # Default to Sponsored Products
    primary_keys: t.ClassVar[list[str]] = ["targetId"]
    replication_key = "lastUpdatedDateTime"
    schema_filepath = SCHEMAS_DIR / "targets.json"
    #parent_stream_type = AdGroupsStream
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
    #parent_stream_type = AdGroupsStream
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


class SearchTermReportStream(AmazonADsStream):
    """Search Term report stream."""
    
    name = "search_term_reports"
    path = "/reporting/reports"
    primary_keys = ["campaignId", "date", "searchTerm"]
    replication_key = "date"
    schema_filepath = SCHEMAS_DIR / "search_term_reports.json"
    #parent_stream_type = CampaignsStream
    method = "POST"
    records_jsonpath = "$.rows[*]"
    
    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {
            "Content-Type": "application/vnd.createasyncreport.v3+json",
            "Accept": "application/vnd.createasyncreport.v3+json",
        }
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_request_body(self, context: dict | None, next_page_token: t.Any | None) -> dict:
        """Return a dictionary to be sent in the request body."""
        return {
            "reportType": "searchTerm",
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS",
                "groupBy": ["searchTerm", "campaignId", "adGroupId"],
                "timeUnit": "DAILY",
                "format": "JSON"
            },
            "startDate": self.get_starting_timestamp(context),
            "endDate": self.get_ending_timestamp(context)
        }

    def get_path(self, context: dict | None) -> str:
        """Return the API endpoint path."""
        return "/reporting/reports"


class AdvertisedProductReportStream(AmazonADsStream):
    """Advertised Product report stream."""
    
    name = "advertised_product_reports"
    path = "/reporting/reports"
    primary_keys = ["campaignId", "date", "advertisedAsin"]
    replication_key = "date"
    schema_filepath = SCHEMAS_DIR / "advertised_product_reports.json"
    method = "POST"
    records_jsonpath = "$.rows[*]"
    
    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        # Prvo dohvatimo access token
        access_token = self.authenticator.access_token
        if not access_token:
            raise Exception("No access token available")
        
        # Log actual token for debugging
        logger.info(f"Access token: {access_token}")
        
        headers = {
            "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
            "Accept": "application/vnd.createasyncreportrequest.v3+json",
            "Amazon-Advertising-API-ClientId": self.config.get("client_id"),
            "Amazon-Advertising-API-Scope": self.config.get("profile_id"),
            "Authorization": "Bearer " + access_token
        }
        
        # Log complete headers
        logger.info(f"Complete headers: {headers}")
        
        return headers

    def get_request_body(self, context: dict | None, next_page_token: t.Any | None) -> dict:
        """Return a dictionary to be sent in the request body."""
        start_date = self.get_starting_timestamp(context)
        end_date = self.get_ending_timestamp(context)
        
        return {
            "name": f"SP advertised product report {start_date}-{end_date}",
            "startDate": start_date,
            "endDate": end_date,
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS",
                "groupBy": ["advertiser"],
                "columns": [
                    "impressions", "clicks", "cost", "campaignId", "advertisedAsin",
                    "date", "startDate", "endDate", "campaignName", "adGroupName",
                    "adGroupId", "adId", "addToList", "qualifiedBorrows",
                    "royaltyQualifiedBorrows", "portfolioId", "costPerClick",
                    "clickThroughRate", "spend", "campaignBudgetCurrencyCode",
                    "campaignBudgetAmount", "campaignBudgetType", "campaignStatus",
                    "advertisedSku", "purchases1d", "purchases7d", "purchases14d",
                    "purchases30d", "purchasesSameSku1d", "purchasesSameSku7d",
                    "purchasesSameSku14d", "purchasesSameSku30d", "unitsSoldClicks1d",
                    "unitsSoldClicks7d", "unitsSoldClicks14d", "unitsSoldClicks30d",
                    "sales1d", "sales7d", "sales14d", "sales30d",
                    "attributedSalesSameSku1d", "attributedSalesSameSku7d",
                    "attributedSalesSameSku14d", "attributedSalesSameSku30d",
                    "salesOtherSku7d", "unitsSoldSameSku1d", "unitsSoldSameSku7d",
                    "unitsSoldSameSku14d", "unitsSoldSameSku30d", "unitsSoldOtherSku7d",
                    "kindleEditionNormalizedPagesRead14d",
                    "kindleEditionNormalizedPagesRoyalties14d", "acosClicks7d",
                    "acosClicks14d", "roasClicks7d", "roasClicks14d"
                ],
                "reportTypeId": "spAdvertisedProduct",
                "timeUnit": "SUMMARY",  # Changed from DAILY to SUMMARY as per documentation
                "format": "GZIP_JSON"
            }
        }

    def validate_response(self, response: requests.Response) -> None:
        """Additional response validation."""
        super().validate_response(response)  # First run parent validation
        
        # Add specific validation for report API if needed
        if response.status_code == 400:
            error = response.json()
            if "message" in error:
                raise RetriableAPIError(
                    f"Report API Error: {error['message']}", 
                    response
                )

    def get_path(self, context: dict | None) -> str:
        """Return the API endpoint path."""
        return "/reporting/reports"


class PurchasedProductReportStream(AmazonADsStream):
    """Purchased Product report stream."""
    
    name = "purchased_product_reports"
    path = "/reporting/reports"
    primary_keys = ["campaignId", "date", "asin"]
    replication_key = "date"
    schema_filepath = SCHEMAS_DIR / "purchased_product_reports.json"
    #parent_stream_type = CampaignsStream
    method = "POST"
    records_jsonpath = "$.rows[*]"
    
    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {
            "Content-Type": "application/vnd.createasyncreport.v3+json",
            "Accept": "application/vnd.createasyncreport.v3+json",
        }
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_request_body(self, context: dict | None, next_page_token: t.Any | None) -> dict:
        """Return a dictionary to be sent in the request body."""
        return {
            "reportType": "purchasedProduct",
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS",
                "groupBy": ["asin", "campaignId", "adGroupId"],
                "timeUnit": "DAILY",
                "format": "JSON"
            },
            "startDate": self.get_starting_timestamp(context),
            "endDate": self.get_ending_timestamp(context)
        }

    def get_path(self, context: dict | None) -> str:
        """Return the API endpoint path."""
        return "/reporting/reports"


class GrossAndInvalidTrafficReportStream(AmazonADsStream):
    """Gross and Invalid Traffic report stream."""
    
    name = "gross_and_invalid_traffic_reports"
    path = "/reporting/reports"
    primary_keys = ["campaignId", "date"]
    replication_key = "date"
    schema_filepath = SCHEMAS_DIR / "gross_and_invalid_traffic_reports.json"
    method = "POST"
    records_jsonpath = "$.rows[*]"
    
    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {
            "Content-Type": "application/vnd.createasyncreport.v3+json",
            "Accept": "application/vnd.createasyncreport.v3+json",
        }
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_request_body(self, context: dict | None, next_page_token: t.Any | None) -> dict:
        """Return a dictionary to be sent in the request body."""
        return {
            "reportType": "grossAndInvalidTraffic",
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS",
                "groupBy": ["campaignId", "adGroupId"],
                "timeUnit": "DAILY",
                "format": "JSON"
            },
            "startDate": self.get_starting_timestamp(context),
            "endDate": self.get_ending_timestamp(context)
        }

    def get_path(self, context: dict | None) -> str:
        """Return the API endpoint path."""
        return "/reporting/reports"
