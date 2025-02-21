"""Stream type classes for tap-amazonads."""

from __future__ import annotations

import typing as t
from pathlib import Path
from singer_sdk import typing as th
import requests
import logging
import json

from tap_amazonads.client import AmazonADsStream
from tap_amazonads.auth import AmazonADsAuthenticator

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
    method = "POST"
    
    def __init__(self, *args, **kwargs):
        """Initialize the stream."""
        super().__init__(*args, **kwargs)
        logger.info(f"Stream initialized with authenticator: {self.authenticator}")

    def request_records(self, context: dict | None) -> t.Iterable[dict]:
        """Request records from REST endpoint(s)."""
        logger.info("\n=== Starting request_records ===")
        
        if not self.authenticator:
            logger.error("No authenticator found!")
            raise Exception("Authenticator not initialized")
            
        if not hasattr(self.authenticator, 'access_token'):
            logger.error(f"Authenticator type: {type(self.authenticator)}")
            logger.error(f"Authenticator attributes: {dir(self.authenticator)}")
            raise Exception("Authenticator has no access_token attribute")
            
        access_token = self.authenticator.access_token
        if not access_token:
            logger.error("No access token available")
            raise Exception("Access token not available")
            
        logger.info("Authentication check passed")
        logger.info(f"Access token (first 20 chars): {access_token[:20]}...")
        
        prepared_request = self.prepare_request(context, None)
        
        # Logujemo kompletan request kao CURL komandu
        curl_command = f"""
curl --location --request {prepared_request.method} '{prepared_request.url}' \\
--header 'Content-Type: {prepared_request.headers.get("Content-Type", "")}' \\
--header 'Accept: {prepared_request.headers.get("Accept", "")}' \\
--header 'Amazon-Advertising-API-ClientId: {prepared_request.headers.get("Amazon-Advertising-API-ClientId", "")}' \\
--header 'Amazon-Advertising-API-Scope: {prepared_request.headers.get("Amazon-Advertising-API-Scope", "")}' \\
--header 'Authorization: Bearer {access_token}' \\
--data-raw '{prepared_request.body.decode() if prepared_request.body else ""}'
"""
        logger.info(f"Equivalent CURL command:\n{curl_command}")
        
        response = self._request(prepared_request, context)
        
        logger.info("\n=== Response Details ===")
        logger.info(f"Response status code: {response.status_code}")
        logger.info(f"Response headers: {dict(response.headers)}")
        logger.info(f"Response body: {response.text}")
        logger.info("=== End Response Details ===\n")
        
        if response.status_code != 200:
            raise Exception(f"Report request failed: {response.text}")
            
        report_info = response.json()
        logger.info(f"Successfully created report request: {report_info}")
        
        yield from []

    def prepare_request(self, context: dict | None, next_page_token: t.Any | None) -> requests.PreparedRequest:
        """Prepare a request object."""
        logger.info("\n=== Preparing Request ===")
        
        http_method = self.method
        url = self.get_url(context)
        headers = self.http_headers
        
        body = {
            "name": "SP search term report",
            "startDate": "2025-02-10",
            "endDate": "2025-02-10",
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS",
                "groupBy": ["searchTerm"],
                "columns": [
                    "impressions",
                    "clicks",
                    "cost",
                    "campaignId",
                    "adGroupId",
                    "date",
                    "targeting",
                    "searchTerm",
                    "keywordType",
                    "keywordId",
                    "keyword",
                    "matchType"
                ],
                "filters": [
                    {
                        "field": "keywordType",
                        "values": [
                            "BROAD",
                            "PHRASE",
                            "EXACT",
                            "TARGETING_EXPRESSION",
                            "TARGETING_EXPRESSION_PREDEFINED"
                        ]
                    }
                ],
                "reportTypeId": "spSearchTerm",
                "timeUnit": "DAILY",
                "format": "GZIP_JSON"
            }
        }
        
        logger.info("Request details:")
        logger.info(f"URL: {url}")
        logger.info(f"Method: {http_method}")
        logger.info(f"Headers: {headers}")
        logger.info(f"Body: {json.dumps(body, indent=2)}")
        
        request = requests.Request(
            method=http_method,
            url=url,
            headers=headers,
            json=body
        )
        
        prepared_request = request.prepare()
        logger.info("\n=== Prepared Request Details ===")
        logger.info(f"Final URL: {prepared_request.url}")
        logger.info(f"Final method: {prepared_request.method}")
        logger.info(f"Final headers: {prepared_request.headers}")
        logger.info(f"Final body: {prepared_request.body}")
        logger.info("=== End Prepared Request Details ===\n")
        
        return prepared_request

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = super().http_headers
        headers.update({
            "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
            "Accept": "application/vnd.createasyncreportrequest.v3+json",
            "Amazon-Advertising-API-ClientId": self.config["client_id"],
            "Amazon-Advertising-API-Scope": self.config["profile_id"],
        })
        
        logger.info("\n=== Headers Details ===")
        safe_headers = headers.copy()
        if 'Authorization' in safe_headers:
            safe_headers['Authorization'] = safe_headers['Authorization'][:20] + '...'
        logger.info(f"Complete headers: {json.dumps(safe_headers, indent=2)}")
        logger.info("=== End Headers Details ===\n")
        
        return headers


class AdvertisedProductReportStream(AmazonADsStream):
    """Advertised Product report stream."""
    
    name = "advertised_product_reports"
    path = "/reporting/reports"
    primary_keys = ["campaignId", "date", "advertisedAsin"]
    replication_key = "date"
    schema_filepath = SCHEMAS_DIR / "advertised_product_reports.json"
    method = "POST"
    
    def __init__(self, *args, **kwargs):
        """Initialize the stream."""
        super().__init__(*args, **kwargs)
        logger.info(f"Stream initialized with authenticator: {self.authenticator}")

    def request_records(self, context: dict | None) -> t.Iterable[dict]:
        """Request records from REST endpoint(s)."""
        logger.info("\n=== Starting request_records ===")
        
        if not self.authenticator:
            logger.error("No authenticator found!")
            raise Exception("Authenticator not initialized")
            
        if not hasattr(self.authenticator, 'access_token'):
            logger.error(f"Authenticator type: {type(self.authenticator)}")
            logger.error(f"Authenticator attributes: {dir(self.authenticator)}")
            raise Exception("Authenticator has no access_token attribute")
            
        access_token = self.authenticator.access_token
        if not access_token:
            logger.error("No access token available")
            raise Exception("Access token not available")
            
        logger.info("Authentication check passed")
        logger.info(f"Access token (first 20 chars): {access_token[:20]}...")
        
        # Kreiramo report request
        prepared_request = self.prepare_request(context, None)
        
        # Logujemo kompletan request kao CURL komandu
        curl_command = f"""
curl --location --request {prepared_request.method} '{prepared_request.url}' \\
--header 'Content-Type: {prepared_request.headers.get("Content-Type", "")}' \\
--header 'Accept: {prepared_request.headers.get("Accept", "")}' \\
--header 'Amazon-Advertising-API-ClientId: {prepared_request.headers.get("Amazon-Advertising-API-ClientId", "")}' \\
--header 'Amazon-Advertising-API-Scope: {prepared_request.headers.get("Amazon-Advertising-API-Scope", "")}' \\
--header 'Authorization: Bearer {access_token}' \\
--data-raw '{prepared_request.body.decode() if prepared_request.body else ""}'
"""
        logger.info(f"Equivalent CURL command:\n{curl_command}")
        
        response = self._request(prepared_request, context)
        
        logger.info("\n=== Response Details ===")
        logger.info(f"Response status code: {response.status_code}")
        logger.info(f"Response headers: {dict(response.headers)}")
        logger.info(f"Response body: {response.text}")
        logger.info("=== End Response Details ===\n")
        
        if response.status_code != 200:
            raise Exception(f"Report request failed: {response.text}")
            
        report_info = response.json()
        logger.info(f"Successfully created report request: {report_info}")
        
        yield from []

    def prepare_request(self, context: dict | None, next_page_token: t.Any | None) -> requests.PreparedRequest:
        """Prepare a request object."""
        logger.info("\n=== Preparing Request ===")
        
        http_method = self.method
        url = self.get_url(context)
        headers = self.http_headers
        
        body = {
            "name": "SP advertised product report",
            "startDate": "2025-02-10",
            "endDate": "2025-02-10",
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS",
                "groupBy": ["advertiser"],
                "columns": [
                    "campaignId",
                    "campaignName",
                    "adGroupId",
                    "adGroupName",
                    "advertisedAsin",
                    "advertisedSku",
                    "impressions",
                    "clicks",
                    "cost",
                    "purchases14d",
                    "sales14d",
                    "unitsSoldClicks14d"
                ],
                "reportTypeId": "spAdvertisedProduct",
                "timeUnit": "SUMMARY",
                "format": "GZIP_JSON"
            }
        }
        
        logger.info("Request details:")
        logger.info(f"URL: {url}")
        logger.info(f"Method: {http_method}")
        logger.info(f"Headers: {headers}")
        logger.info(f"Body: {json.dumps(body, indent=2)}")
        
        request = requests.Request(
            method=http_method,
            url=url,
            headers=headers,
            json=body
        )
        
        prepared_request = request.prepare()
        logger.info("\n=== Prepared Request Details ===")
        logger.info(f"Final URL: {prepared_request.url}")
        logger.info(f"Final method: {prepared_request.method}")
        logger.info(f"Final headers: {prepared_request.headers}")
        logger.info(f"Final body: {prepared_request.body}")
        logger.info("=== End Prepared Request Details ===\n")
        
        return prepared_request

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = super().http_headers
        
        # Dodajemo specifične headere za Amazon Advertising API
        headers.update({
            "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
            "Accept": "application/vnd.createasyncreportrequest.v3+json",
            "Amazon-Advertising-API-ClientId": self.config["client_id"],
            "Amazon-Advertising-API-Scope": self.config["profile_id"],  # Koristimo profile_id
        })
        
        logger.info("\n=== Headers Details ===")
        safe_headers = headers.copy()
        if 'Authorization' in safe_headers:
            safe_headers['Authorization'] = safe_headers['Authorization'][:20] + '...'
        logger.info(f"Complete headers: {json.dumps(safe_headers, indent=2)}")
        logger.info("=== End Headers Details ===\n")
        
        return headers


class PurchasedProductReportStream(AmazonADsStream):
    """Purchased Product report stream."""
    
    name = "purchased_product_reports"
    path = "/reporting/reports"
    primary_keys = ["campaignId", "date", "purchasedAsin"]
    replication_key = "date"
    schema_filepath = SCHEMAS_DIR / "purchased_product_reports.json"
    method = "POST"
    
    def __init__(self, *args, **kwargs):
        """Initialize the stream."""
        super().__init__(*args, **kwargs)
        logger.info(f"Stream initialized with authenticator: {self.authenticator}")

    def request_records(self, context: dict | None) -> t.Iterable[dict]:
        """Request records from REST endpoint(s)."""
        logger.info("\n=== Starting request_records ===")
        
        if not self.authenticator:
            logger.error("No authenticator found!")
            raise Exception("Authenticator not initialized")
            
        if not hasattr(self.authenticator, 'access_token'):
            logger.error(f"Authenticator type: {type(self.authenticator)}")
            logger.error(f"Authenticator attributes: {dir(self.authenticator)}")
            raise Exception("Authenticator has no access_token attribute")
            
        access_token = self.authenticator.access_token
        if not access_token:
            logger.error("No access token available")
            raise Exception("Access token not available")
            
        logger.info("Authentication check passed")
        logger.info(f"Access token (first 20 chars): {access_token[:20]}...")
        
        prepared_request = self.prepare_request(context, None)
        
        # Logujemo kompletan request kao CURL komandu
        curl_command = f"""
curl --location --request {prepared_request.method} '{prepared_request.url}' \\
--header 'Content-Type: {prepared_request.headers.get("Content-Type", "")}' \\
--header 'Accept: {prepared_request.headers.get("Accept", "")}' \\
--header 'Amazon-Advertising-API-ClientId: {prepared_request.headers.get("Amazon-Advertising-API-ClientId", "")}' \\
--header 'Amazon-Advertising-API-Scope: {prepared_request.headers.get("Amazon-Advertising-API-Scope", "")}' \\
--header 'Authorization: Bearer {access_token}' \\
--data-raw '{prepared_request.body.decode() if prepared_request.body else ""}'
"""
        logger.info(f"Equivalent CURL command:\n{curl_command}")
        
        response = self._request(prepared_request, context)
        
        logger.info("\n=== Response Details ===")
        logger.info(f"Response status code: {response.status_code}")
        logger.info(f"Response headers: {dict(response.headers)}")
        logger.info(f"Response body: {response.text}")
        logger.info("=== End Response Details ===\n")
        
        if response.status_code != 200:
            raise Exception(f"Report request failed: {response.text}")
            
        report_info = response.json()
        logger.info(f"Successfully created report request: {report_info}")
        
        yield from []

    def prepare_request(self, context: dict | None, next_page_token: t.Any | None) -> requests.PreparedRequest:
        """Prepare a request object."""
        logger.info("\n=== Preparing Request ===")
        
        http_method = self.method
        url = self.get_url(context)
        headers = self.http_headers
        
        body = {
            "name": "SP purchased product report",
            "startDate": "2025-02-10",
            "endDate": "2025-02-10",
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS",
                "groupBy": ["asin"],
                "columns": [
                    "date",
                    "campaignId",
                    "campaignName",
                    "adGroupId",
                    "adGroupName",
                    "keywordId",
                    "keyword",
                    "keywordType",
                    "advertisedAsin",
                    "purchasedAsin",
                    "advertisedSku",
                    "sales14d",
                    "purchases14d",
                    "unitsSoldClicks14d"
                ],
                "reportTypeId": "spPurchasedProduct",
                "timeUnit": "SUMMARY",
                "format": "GZIP_JSON"
            }
        }
        
        logger.info("Request details:")
        logger.info(f"URL: {url}")
        logger.info(f"Method: {http_method}")
        logger.info(f"Headers: {headers}")
        logger.info(f"Body: {json.dumps(body, indent=2)}")
        
        request = requests.Request(
            method=http_method,
            url=url,
            headers=headers,
            json=body
        )
        
        prepared_request = request.prepare()
        logger.info("\n=== Prepared Request Details ===")
        logger.info(f"Final URL: {prepared_request.url}")
        logger.info(f"Final method: {prepared_request.method}")
        logger.info(f"Final headers: {prepared_request.headers}")
        logger.info(f"Final body: {prepared_request.body}")
        logger.info("=== End Prepared Request Details ===\n")
        
        return prepared_request

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = super().http_headers
        headers.update({
            "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
            "Accept": "application/vnd.createasyncreportrequest.v3+json",
            "Amazon-Advertising-API-ClientId": self.config["client_id"],
            "Amazon-Advertising-API-Scope": self.config["profile_id"],
        })
        
        logger.info("\n=== Headers Details ===")
        safe_headers = headers.copy()
        if 'Authorization' in safe_headers:
            safe_headers['Authorization'] = safe_headers['Authorization'][:20] + '...'
        logger.info(f"Complete headers: {json.dumps(safe_headers, indent=2)}")
        logger.info("=== End Headers Details ===\n")
        
        return headers


class GrossAndInvalidTrafficReportStream(AmazonADsStream):
    """Gross and Invalid Traffic report stream."""
    
    name = "gross_and_invalid_traffic_reports"
    path = "/reporting/reports"
    primary_keys = ["campaignId", "date"]
    replication_key = "date"
    schema_filepath = SCHEMAS_DIR / "gross_and_invalid_traffic_reports.json"
    method = "POST"
    
    def __init__(self, *args, **kwargs):
        """Initialize the stream."""
        super().__init__(*args, **kwargs)
        logger.info(f"Stream initialized with authenticator: {self.authenticator}")

    def request_records(self, context: dict | None) -> t.Iterable[dict]:
        """Request records from REST endpoint(s)."""
        logger.info("\n=== Starting request_records ===")
        
        if not self.authenticator:
            logger.error("No authenticator found!")
            raise Exception("Authenticator not initialized")
            
        if not hasattr(self.authenticator, 'access_token'):
            logger.error(f"Authenticator type: {type(self.authenticator)}")
            logger.error(f"Authenticator attributes: {dir(self.authenticator)}")
            raise Exception("Authenticator has no access_token attribute")
            
        access_token = self.authenticator.access_token
        if not access_token:
            logger.error("No access token available")
            raise Exception("Access token not available")
            
        logger.info("Authentication check passed")
        logger.info(f"Access token (first 20 chars): {access_token[:20]}...")
        
        prepared_request = self.prepare_request(context, None)
        
        # Logujemo kompletan request kao CURL komandu
        curl_command = f"""
curl --location --request {prepared_request.method} '{prepared_request.url}' \\
--header 'Content-Type: {prepared_request.headers.get("Content-Type", "")}' \\
--header 'Accept: {prepared_request.headers.get("Accept", "")}' \\
--header 'Amazon-Advertising-API-ClientId: {prepared_request.headers.get("Amazon-Advertising-API-ClientId", "")}' \\
--header 'Amazon-Advertising-API-Scope: {prepared_request.headers.get("Amazon-Advertising-API-Scope", "")}' \\
--header 'Authorization: Bearer {access_token}' \\
--data-raw '{prepared_request.body.decode() if prepared_request.body else ""}'
"""
        logger.info(f"Equivalent CURL command:\n{curl_command}")
        
        response = self._request(prepared_request, context)
        
        logger.info("\n=== Response Details ===")
        logger.info(f"Response status code: {response.status_code}")
        logger.info(f"Response headers: {dict(response.headers)}")
        logger.info(f"Response body: {response.text}")
        logger.info("=== End Response Details ===\n")
        
        if response.status_code != 200:
            raise Exception(f"Report request failed: {response.text}")
            
        report_info = response.json()
        logger.info(f"Successfully created report request: {report_info}")
        
        yield from []

    def prepare_request(self, context: dict | None, next_page_token: t.Any | None) -> requests.PreparedRequest:
        """Prepare a request object."""
        logger.info("\n=== Preparing Request ===")
        
        http_method = self.method
        url = self.get_url(context)
        headers = self.http_headers
        
        body = {
            "name": "SP Gross and Invalid Traffic",
            "startDate": "2025-02-10",
            "endDate": "2025-02-10",
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS",
                "groupBy": ["campaign"],
                "columns": [
                    "campaignName",
                    "campaignStatus",
                    "clicks",
                    "date",
                    "endDate",
                    "grossClickThroughs",
                    "grossImpressions",
                    "impressions",
                    "invalidClickThroughRate",
                    "invalidClickThroughs",
                    "invalidImpressionRate",
                    "invalidImpressions",
                    "startDate"
                ],
                "reportTypeId": "spGrossAndInvalids",
                "timeUnit": "SUMMARY",
                "format": "GZIP_JSON"
            }
        }
        
        logger.info("Request details:")
        logger.info(f"URL: {url}")
        logger.info(f"Method: {http_method}")
        logger.info(f"Headers: {headers}")
        logger.info(f"Body: {json.dumps(body, indent=2)}")
        
        request = requests.Request(
            method=http_method,
            url=url,
            headers=headers,
            json=body
        )
        
        prepared_request = request.prepare()
        logger.info("\n=== Prepared Request Details ===")
        logger.info(f"Final URL: {prepared_request.url}")
        logger.info(f"Final method: {prepared_request.method}")
        logger.info(f"Final headers: {prepared_request.headers}")
        logger.info(f"Final body: {prepared_request.body}")
        logger.info("=== End Prepared Request Details ===\n")
        
        return prepared_request

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = super().http_headers
        headers.update({
            "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
            "Accept": "application/vnd.createasyncreportrequest.v3+json",
            "Amazon-Advertising-API-ClientId": self.config["client_id"],
            "Amazon-Advertising-API-Scope": self.config["profile_id"],
        })
        
        logger.info("\n=== Headers Details ===")
        safe_headers = headers.copy()
        if 'Authorization' in safe_headers:
            safe_headers['Authorization'] = safe_headers['Authorization'][:20] + '...'
        logger.info(f"Complete headers: {json.dumps(safe_headers, indent=2)}")
        logger.info("=== End Headers Details ===\n")
        
        return headers


class CampaignReportStream(AmazonADsStream):
    """Campaign report stream."""
    
    name = "campaign_reports"
    path = "/reporting/reports"
    primary_keys = ["campaignId", "date"]
    replication_key = "date"
    schema_filepath = SCHEMAS_DIR / "campaign_reports.json"
    method = "POST"
    records_jsonpath = "$.reports[*]"
    
    def __init__(self, *args, **kwargs):
        """Initialize the stream."""
        super().__init__(*args, **kwargs)
        self._authenticator = None  # Dodajemo inicijalizaciju
        logger.info(f"Stream initialized with authenticator: {self.authenticator}")

    @property
    def authenticator(self) -> AmazonADsAuthenticator:
        """Return a new authenticator object."""
        if not self._authenticator:
            self._authenticator = AmazonADsAuthenticator.create_for_stream(self)
        return self._authenticator

    def request_records(self, context: dict | None) -> t.Iterable[dict]:
        """Request records from REST endpoint(s)."""
        logger.info("\n=== Starting request_records ===")
        
        if not self.authenticator:
            logger.error("No authenticator found!")
            raise Exception("Authenticator not initialized")
            
        if not hasattr(self.authenticator, 'access_token'):
            logger.error(f"Authenticator type: {type(self.authenticator)}")
            logger.error(f"Authenticator attributes: {dir(self.authenticator)}")
            raise Exception("Authenticator has no access_token attribute")
            
        access_token = self.authenticator.access_token
        if not access_token:
            logger.error("No access token available")
            raise Exception("Access token not available")
            
        logger.info("Authentication check passed")
        logger.info(f"Access token (first 20 chars): {access_token[:20]}...")
        
        prepared_request = self.prepare_request(context, None)
        
        # Log complete request as CURL command
        curl_command = f"""
curl --location --request {prepared_request.method} '{prepared_request.url}' \\
--header 'Content-Type: {prepared_request.headers.get("Content-Type", "")}' \\
--header 'Accept: {prepared_request.headers.get("Accept", "")}' \\
--header 'Amazon-Advertising-API-ClientId: {prepared_request.headers.get("Amazon-Advertising-API-ClientId", "")}' \\
--header 'Amazon-Advertising-API-Scope: {prepared_request.headers.get("Amazon-Advertising-API-Scope", "")}' \\
--header 'Authorization: Bearer {access_token}' \\
--data-raw '{prepared_request.body.decode() if prepared_request.body else ""}'
"""
        logger.info(f"Equivalent CURL command:\n{curl_command}")
        
        response = self._request(prepared_request, context)
        
        logger.info("\n=== Response Details ===")
        logger.info(f"Response status code: {response.status_code}")
        logger.info(f"Response headers: {dict(response.headers)}")
        logger.info(f"Response body: {response.text}")
        logger.info("=== End Response Details ===\n")
        
        if response.status_code != 200:
            raise Exception(f"Report request failed: {response.text}")
            
        report_info = response.json()
        logger.info(f"Successfully created report request: {report_info}")
        
        yield from []

    def prepare_request(self, context: dict | None, next_page_token: t.Any | None) -> requests.PreparedRequest:
        """Prepare a request object."""
        logger.info("\n=== Preparing Request ===")
        
        http_method = self.method
        url = self.get_url(context)
        headers = self.http_headers
        
        body = {
            "name": "SP Campaign Report",
            "startDate": "2025-02-10",
            "endDate": "2025-02-10",
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS",
                "groupBy": ["campaign"],
                "columns": [
                    "campaignName",
                    "campaignId",
                    "campaignStatus",
                    "campaignBudgetAmount",
                    "campaignBudgetType",
                    "campaignBudgetCurrencyCode",
                    "impressions",
                    "clicks",
                    "cost",
                    "costPerClick",
                    "clickThroughRate",
                    "purchases14d",
                    "sales14d",
                    "unitsSoldClicks14d",
                    "topOfSearchImpressionShare",
                    "date",
                    "startDate",
                    "endDate"
                ],
                "reportTypeId": "spCampaigns",
                "timeUnit": "SUMMARY",
                "format": "GZIP_JSON"
            }
        }
        
        logger.info("Request details:")
        logger.info(f"URL: {url}")
        logger.info(f"Method: {http_method}")
        logger.info(f"Headers: {headers}")
        logger.info(f"Body: {json.dumps(body, indent=2)}")
        
        request = requests.Request(
            method=http_method,
            url=url,
            headers=headers,
            json=body
        )
        
        prepared_request = request.prepare()
        logger.info("\n=== Prepared Request Details ===")
        logger.info(f"Final URL: {prepared_request.url}")
        logger.info(f"Final method: {prepared_request.method}")
        logger.info(f"Final headers: {prepared_request.headers}")
        logger.info(f"Final body: {prepared_request.body}")
        logger.info("=== End Prepared Request Details ===\n")
        
        return prepared_request

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = super().http_headers
        headers.update({
            "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
            "Accept": "application/vnd.createasyncreportrequest.v3+json",
            "Amazon-Advertising-API-ClientId": self.config["client_id"],
            "Amazon-Advertising-API-Scope": self.config["profile_id"],
        })
        
        logger.info("\n=== Headers Details ===")
        safe_headers = headers.copy()
        if 'Authorization' in safe_headers:
            safe_headers['Authorization'] = safe_headers['Authorization'][:20] + '...'
        logger.info(f"Complete headers: {json.dumps(safe_headers, indent=2)}")
        logger.info("=== End Headers Details ===\n")
        
        return headers
