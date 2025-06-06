"""Stream type classes for tap-amazonads."""

from __future__ import annotations

import typing as t
from pathlib import Path
from singer_sdk import typing as th
import requests
import logging
import json
import time
import random
import uuid
import gzip
import io
from datetime import datetime, timezone

from tap_amazonads.client import AmazonADsStream
from tap_amazonads.auth import AmazonADsAuthenticator, AmazonADsNonReportAuthenticator

SCHEMAS_DIR = Path(__file__).parent / "schemas"

logger = logging.getLogger(__name__)

class BaseReportStream(AmazonADsStream):
    """Base class for all report streams."""

    def get_report_status(self, report_id: str) -> dict:
        """Get the status of a report."""
        url = f"https://advertising-api.amazon.com/reporting/reports/{report_id}"
        
        headers = {
            "Content-Type": "application/json",
            "Amazon-Advertising-API-ClientId": self.config["client_id"],
            "Amazon-Advertising-API-Scope": self.config["profile_id"],
            "Authorization": f"Bearer {self.authenticator.access_token}"
        }
        
        request = requests.Request(
            method="GET",
            url=url,
            headers=headers
        )
        prepared_request = request.prepare()
        
        response = self._request(prepared_request)
        return response.json()

    def download_and_process_report(self, report_url: str) -> list[dict]:
        """Download, unzip and process report from S3."""
        logger.info(f"Downloading report from URL: {report_url}")
        
        try:
            # Download the gzipped file
            response = requests.get(report_url)
            response.raise_for_status()
            
            # Decompress the gzipped content
            with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
                json_content = gz.read().decode('utf-8')
            
            # Parse JSON content
            records = json.loads(json_content)
            
            logger.info("Successfully processed report content:")
            logger.info(f"Number of records: {len(records)}")
            logger.info("First record sample:")
            if records:
                logger.info(json.dumps(records[0], indent=2))
            
            return records
            
        except Exception as e:
            logger.error(f"Error processing report: {str(e)}")
            raise

    def process_report(self, report_info: dict) -> t.Iterable[dict]:
        """Process report after initial creation."""
        report_id = report_info["reportId"]
        max_attempts = 200
        attempt = 0
        initial_wait = 360  # 6 minuta

        # Prvi pokušaj - čekamo 6 minuta
        if attempt == 0:
            wait_time = initial_wait
            logger.info(f"Initial wait of {wait_time} seconds before first check...")
            time.sleep(wait_time)
            
            # Provjera i osvježavanje tokena prije API poziva
            self._refresh_token_if_needed()
            
            report_status = self.get_report_status(report_id)
            logger.info(f"Report status: {report_status['status']}")
            
            if report_status["status"] == "COMPLETED":
                logger.info(f"Report completed! URL: {report_status['url']}")
                return self.download_and_process_report(report_status['url'])
            elif report_status["status"] == "FAILED":
                error_msg = f"Report generation failed: {report_status.get('failureReason')}"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            attempt += 1

        # Naredni pokušaji - čekamo po minut
        while attempt < max_attempts:
            wait_time = 60  # 1 minut za sve ostale pokušaje
            
            logger.info(f"Waiting {wait_time} seconds before checking report status...")
            time.sleep(wait_time)
            
            # Provjera i osvježavanje tokena prije svakog API poziva
            self._refresh_token_if_needed()
            
            report_status = self.get_report_status(report_id)
            logger.info(f"Report status: {report_status['status']}")
            
            if report_status["status"] == "COMPLETED":
                logger.info(f"Report completed! URL: {report_status['url']}")
                return self.download_and_process_report(report_status['url'])
            elif report_status["status"] == "FAILED":
                error_msg = f"Report generation failed: {report_status.get('failureReason')}"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            attempt += 1
            
        error_msg = f"Reached maximum attempts waiting for report. Last status: {report_status['status']}"
        logger.warning(error_msg)
        raise Exception(error_msg)

    def _refresh_token_if_needed(self):
        """Check and refresh token if it's close to expiration."""
        if not hasattr(self.authenticator, '_token_expiry'):
            # Ako nema _token_expiry, osvježi token
            logger.info("No token expiry found, refreshing token...")
            self.authenticator.refresh_access_token()
            return

        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        
        # Ako je token blizu isteka (manje od 5 minuta), osvježi ga
        if (self.authenticator._token_expiry - now).total_seconds() < 300:
            logger.info("Token is about to expire, refreshing...")
            self.authenticator.refresh_access_token()

    def get_report_dates(self) -> tuple[str, str]:
        """Return start and end dates for report.
        
        Returns:
            Tuple containing start_date and end_date in YYYY-MM-DD format
        """
        # Get end_date (current date in UTC)
        end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        # Get start_date from config or use end_date if not specified
        start_date = self.config.get("start_date")
        if start_date:
            # If start_date is datetime object, convert to string
            if isinstance(start_date, datetime):
                start_date = start_date.strftime("%Y-%m-%d")
        else:
            start_date = end_date
            
        logger.info(f"Report date range: {start_date} to {end_date}")
        return start_date, end_date

class CampaignsStream(AmazonADsStream):
    """Campaigns stream."""
    
    name = "campaigns"
    path = "/sp/campaigns/list"
    primary_keys: t.ClassVar[list[str]] = ["campaignId"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaigns.json"
    method = "POST"
    records_jsonpath = "$.campaigns[*]"
    
    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {
            "Content-Type": "application/vnd.spcampaign.v3+json",
            "Accept": "application/vnd.spcampaign.v3+json",
            "Amazon-Advertising-API-ClientId": self.config["client_id"],
            "Amazon-Advertising-API-Scope": self.config["profile_id"],
        }
        if self.authenticator:
            headers["Authorization"] = f"Bearer {self.authenticator.access_token}"
        return headers

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: t.Any | None,
    ) -> dict | None:
        """Prepare request payload."""
        return {}  # Return empty dict as required by the API

    def get_request_body(self, context: dict | None, next_page_token: t.Any | None) -> dict | None:
        """Return a dictionary to be sent in the request body."""
        return {}

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

        # Dodajemo logging
        logger.info("\n=== Campaigns Stream Request Details ===")
        logger.info(f"URL: {url}")
        logger.info(f"Method: {http_method}")
        logger.info(f"Params: {params}")
        logger.info(f"Headers: {headers}")
        logger.info(f"Request Body: {request_data}")
        logger.info("=== End Campaigns Stream Request Details ===\n")

        prepared_request = self.build_prepared_request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            json=request_data,
        )

        # Logujemo i prepared request
        logger.info("\n=== Prepared Request Details ===")
        logger.info(f"Final URL: {prepared_request.url}")
        logger.info(f"Final Method: {prepared_request.method}")
        logger.info(f"Final Headers: {prepared_request.headers}")
        logger.info(f"Final Body: {prepared_request.body}")
        logger.info("=== End Prepared Request Details ===\n")

        return prepared_request

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
    replication_key = None
    records_jsonpath = "$.adGroups[*]"
    method = "POST"
    schema_filepath = SCHEMAS_DIR / "ad_groups.json"
    
    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {
            "Content-Type": "application/vnd.spadGroup.v3+json",
            "Accept": "application/vnd.spadGroup.v3+json",
            "Amazon-Advertising-API-ClientId": self.config["client_id"],
            "Amazon-Advertising-API-Scope": self.config["profile_id"],
        }
        if self.authenticator:
            headers["Authorization"] = f"Bearer {self.authenticator.access_token}"
        return headers

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: t.Any | None,
    ) -> dict | None:
        """Prepare request payload."""
        return {}  # Return empty dict as required by the API

    def get_request_body(self, context: dict | None, next_page_token: t.Any | None) -> dict | None:
        """Return a dictionary to be sent in the request body."""
        return {}

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

    def prepare_request(self, context: dict | None, next_page_token: t.Any | None) -> requests.PreparedRequest:
        """Prepare a request object for the REST API."""
        http_method = self.method
        url: str = self.get_url(context)
        params: dict = self.get_url_params(context, next_page_token)
        request_data = self.get_request_body(context, next_page_token)
        headers = self.http_headers

        # Dodajemo logging
        logger.info("\n=== Ad Groups Stream Request Details ===")
        logger.info(f"URL: {url}")
        logger.info(f"Method: {http_method}")
        logger.info(f"Params: {params}")
        logger.info(f"Headers: {headers}")
        logger.info(f"Request Body: {request_data}")
        logger.info("=== End Ad Groups Stream Request Details ===\n")

        prepared_request = self.build_prepared_request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            json=request_data,
        )

        # Logujemo i prepared request
        logger.info("\n=== Prepared Request Details ===")
        logger.info(f"Final URL: {prepared_request.url}")
        logger.info(f"Final Method: {prepared_request.method}")
        logger.info(f"Final Headers: {prepared_request.headers}")
        logger.info(f"Final Body: {prepared_request.body}")
        logger.info("=== End Prepared Request Details ===\n")

        return prepared_request

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
    path = "/sp/targets/list"
    primary_keys: t.ClassVar[list[str]] = ["targetId"]
    replication_key = "lastUpdatedDateTime"
    schema_filepath = SCHEMAS_DIR / "targets.json"
    method = "POST"
    records_jsonpath = "$.targetingClauses[*]"
    
    @property
    def authenticator(self) -> AmazonADsNonReportAuthenticator:
        """Return a new authenticator object."""
        return AmazonADsNonReportAuthenticator(self.config)

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: t.Any | None,
    ) -> dict | None:
        """Prepare request payload."""
        return {}  # Return empty dict as required by the API

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {
            "Content-Type": "application/vnd.sptargetingClause.v3+json",
            "Accept": "application/vnd.sptargetingClause.v3+json",
            "Amazon-Advertising-API-ClientId": self.config["client_id"],
            "Amazon-Advertising-API-Scope": self.config["profile_id"],
        }
        # Add Authorization header from authenticator
        if self.authenticator:
            headers["Authorization"] = f"Bearer {self.authenticator.access_token}"
        
        logger.info("Request headers prepared: %s", headers)
        return headers

    def get_request_body(self, context: dict | None, next_page_token: t.Any | None) -> dict | None:
        """Return a dictionary to be sent in the request body."""
        # API expects an empty object for this endpoint
        return {}

    def prepare_request(
        self, 
        context: dict | None, 
        next_page_token: t.Any | None
    ) -> requests.PreparedRequest:
        """Prepare a request object for the REST API."""
        http_method = self.method
        url: str = self.get_url(context)
        params: dict = {}
        request_data = self.get_request_body(context, next_page_token)
        headers = self.http_headers

        logger.info("Preparing request with:")
        logger.info(f"URL: {url}")
        logger.info(f"Method: {http_method}")
        logger.info(f"Headers: {headers}")
        logger.info(f"Body: {request_data}")

        prepared_request = self.build_prepared_request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            json=request_data,
        )

        logger.info("Prepared request details:")
        logger.info(f"Final URL: {prepared_request.url}")
        logger.info(f"Final Method: {prepared_request.method}")
        logger.info(f"Final Headers: {prepared_request.headers}")
        logger.info(f"Final Body: {prepared_request.body}")

        return prepared_request

    def get_path(self, context: dict | None) -> str:
        """Return the API endpoint path."""
        ad_product = context.get("adProduct", "SPONSORED_PRODUCTS").lower() if context else "sponsored_products"
        path = self.path
        
        if ad_product == "sponsored_products":
            path = "/sp/targets/list"
        elif ad_product == "sponsored_brands":
            path = "/sb/targets/list"
        elif ad_product == "sponsored_display":
            path = "/sd/targets"
            self.method = "GET"
        
        logger.info(f"Using path: {path} for ad_product: {ad_product}")
        return path


class AdsStream(AmazonADsStream):
    """Ads stream."""
    
    name = "ads"
    path = "/sp/productAds/list"
    primary_keys = ["adId"]
    replication_key = None
    records_jsonpath = "$.productAds[*]"
    method = "POST"
    schema_filepath = SCHEMAS_DIR / "ads.json"
    
    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {
            "Content-Type": "application/vnd.spproductAd.v3+json",
            "Accept": "application/vnd.spproductAd.v3+json",
            "Amazon-Advertising-API-ClientId": self.config["client_id"],
            "Amazon-Advertising-API-Scope": self.config["profile_id"],
        }
        if self.authenticator:
            headers["Authorization"] = f"Bearer {self.authenticator.access_token}"
        return headers

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: t.Any | None,
    ) -> dict | None:
        """Prepare request payload."""
        return {}  # Return empty dict as required by the API

    def get_request_body(self, context: dict | None, next_page_token: t.Any | None) -> dict | None:
        """Return a dictionary to be sent in the request body."""
        return {}

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

    def prepare_request(self, context: dict | None, next_page_token: t.Any | None) -> requests.PreparedRequest:
        """Prepare a request object for the REST API."""
        http_method = self.method
        url: str = self.get_url(context)
        params: dict = self.get_url_params(context, next_page_token)
        request_data = self.get_request_body(context, next_page_token)
        headers = self.http_headers

        # Dodajemo logging
        logger.info("\n=== Ads Stream Request Details ===")
        logger.info(f"URL: {url}")
        logger.info(f"Method: {http_method}")
        logger.info(f"Params: {params}")
        logger.info(f"Headers: {headers}")
        logger.info(f"Request Body: {request_data}")
        logger.info("=== End Ads Stream Request Details ===\n")

        prepared_request = self.build_prepared_request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            json=request_data,
        )

        # Logujemo i prepared request
        logger.info("\n=== Prepared Request Details ===")
        logger.info(f"Final URL: {prepared_request.url}")
        logger.info(f"Final Method: {prepared_request.method}")
        logger.info(f"Final Headers: {prepared_request.headers}")
        logger.info(f"Final Body: {prepared_request.body}")
        logger.info("=== End Prepared Request Details ===\n")

        return prepared_request

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


class SearchTermReportStream(BaseReportStream):
    """Search term report stream."""
    
    name = "search_term_reports"
    path = "/reporting/reports"
    primary_keys = ["campaignId", "date"]
    replication_key = "date"
    schema_filepath = SCHEMAS_DIR / "search_term_reports.json"
    method = "POST"
    
    def __init__(self, tap=None):
        """Initialize the stream.
        
        Args:
            tap: The parent tap instance
        """
        super().__init__(tap=tap)
        self.tap = tap  # Explicitly store tap reference
        if tap:
            logger.info(f"Stream {self.name} initialized with tap config: {tap.config}")

    @property
    def authenticator(self):
        """Return a new authenticator object."""
        if not self.tap:
            raise Exception(f"Stream {self.name} has no tap instance")
        return AmazonADsAuthenticator.create_for_stream(self)

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
        
        yield from self.process_report(report_info)

    def prepare_request(self, context: dict | None, next_page_token: t.Any | None) -> requests.PreparedRequest:
        """Prepare a request object."""
        logger.info("\n=== Preparing Request ===")
        
        http_method = self.method
        url = self.get_url(context)
        headers = self.http_headers
        
        from datetime import datetime
        
        start_date = self.config.get("start_date")
        if isinstance(start_date, datetime):
            start_date = start_date.strftime("%Y-%m-%d")
        elif not start_date:
            start_date = "2025-02-10"  # Default ako nema start_date
            
        end_date = self.config.get("end_date")
        if isinstance(end_date, datetime):
            end_date = end_date.strftime("%Y-%m-%d")
        elif not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        
        body = {
            "name": "SP search term report",
            "startDate": start_date,
            "endDate": end_date,
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

    def get_records(self, context: dict | None) -> t.Iterable[dict]:
        """Get records from the source."""
        report_request = self.prepare_request(context, None)
        response = self._request(prepared_request=report_request)
        report_info = response.json()
        
        return self.process_report(report_info)


class AdvertisedProductReportStream(BaseReportStream):
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
        
        yield from self.process_report(report_info)

    def prepare_request(self, context: dict | None, next_page_token: t.Any | None) -> requests.PreparedRequest:
        """Prepare a request object."""
        logger.info("\n=== Preparing Request ===")
        
        http_method = self.method
        url = self.get_url(context)
        headers = self.http_headers
        
        from datetime import datetime
        
        start_date = self.config.get("start_date")
        if isinstance(start_date, datetime):
            start_date = start_date.strftime("%Y-%m-%d")
        elif not start_date:
            start_date = "2025-02-10"  # Default ako nema start_date
            
        end_date = self.config.get("end_date")
        if isinstance(end_date, datetime):
            end_date = end_date.strftime("%Y-%m-%d")
        elif not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        
        body = {
            "name": "SP advertised product report",
            "startDate": start_date,
            "endDate": end_date,
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS",
                "groupBy": ["advertiser", "campaign", "advertised_asin"],
                "columns": [
                    "campaignId",
                    "campaignName",
                    "advertisedAsin",
                    "impressions",
                    "clicks",
                    "cost",
                    "date",
                    "purchases14d",
                    "unitsSoldClicks14d",
                    "sales14d"
                ],
                "reportTypeId": "spAdvertisedProduct",
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

    def get_records(self, context: dict | None) -> t.Iterable[dict]:
        """Get records from the source."""
        report_request = self.prepare_request(context, None)
        response = self._request(prepared_request=report_request)
        report_info = response.json()
        
        return self.process_report(report_info)


class PurchasedProductReportStream(BaseReportStream):
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
        
        yield from self.process_report(report_info)

    def prepare_request(self, context: dict | None, next_page_token: t.Any | None) -> requests.PreparedRequest:
        """Prepare a request object."""
        logger.info("\n=== Preparing Request ===")
        
        http_method = self.method
        url = self.get_url(context)
        headers = self.http_headers
        
        from datetime import datetime
        
        start_date = self.config.get("start_date")
        if isinstance(start_date, datetime):
            start_date = start_date.strftime("%Y-%m-%d")
        elif not start_date:
            start_date = "2025-02-10"  # Default ako nema start_date
            
        end_date = self.config.get("end_date")
        if isinstance(end_date, datetime):
            end_date = end_date.strftime("%Y-%m-%d")
        elif not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        
        body = {
            "name": "SP purchased product report",
            "startDate": start_date,
            "endDate": end_date,
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS",
                "groupBy": ["asin"],
                "columns": [
                    "startDate",
                    "endDate",
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

    def get_records(self, context: dict | None) -> t.Iterable[dict]:
        """Get records from the source."""
        report_request = self.prepare_request(context, None)
        response = self._request(prepared_request=report_request)
        report_info = response.json()
        
        return self.process_report(report_info)


class GrossAndInvalidTrafficReportStream(BaseReportStream):
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
        
        yield from self.process_report(report_info)

    def prepare_request(self, context: dict | None, next_page_token: t.Any | None) -> requests.PreparedRequest:
        """Prepare a request object."""
        logger.info("\n=== Preparing Request ===")
        
        http_method = self.method
        url = self.get_url(context)
        headers = self.http_headers
        
        from datetime import datetime
        
        start_date = self.config.get("start_date")
        if isinstance(start_date, datetime):
            start_date = start_date.strftime("%Y-%m-%d")
        elif not start_date:
            start_date = "2025-02-10"  # Default ako nema start_date
            
        end_date = self.config.get("end_date")
        if isinstance(end_date, datetime):
            end_date = end_date.strftime("%Y-%m-%d")
        elif not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        
        body = {
            "name": "SP Gross and Invalid Traffic",
            "startDate": start_date,
            "endDate": end_date,
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

    def get_records(self, context: dict | None) -> t.Iterable[dict]:
        """Get records from the source."""
        report_request = self.prepare_request(context, None)
        response = self._request(prepared_request=report_request)
        report_info = response.json()
        
        return self.process_report(report_info)


class CampaignReportStream(BaseReportStream):
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
        self._authenticator = None
        self.request_headers = {}  # Inicijaliziramo request_headers
        super().__init__(*args, **kwargs)
        logger.info(f"Stream initialized with authenticator: {self.authenticator}")

    @property
    def authenticator(self) -> AmazonADsAuthenticator:
        """Return a new authenticator object."""
        if not self._authenticator:
            logger.info("Creating new authenticator for stream")
            logger.info(f"Stream config keys: {list(self.config.keys())}")
            logger.info(f"Stream config values: {self.config}")
            logger.info(f"Parent tap config keys: {list(self.tap.config.keys())}")
            logger.info(f"Parent tap config values: {self.tap.config}")
            
            self._authenticator = AmazonADsAuthenticator(self.config)
            logger.info(f"Created new authenticator: {type(self._authenticator)}")
            logger.info(f"Authenticator attributes: {dir(self._authenticator)}")
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
        
        # Dodajemo dugo inicijalno čekanje (2-5 minuta)
        initial_wait = random.uniform(120, 300)
        logger.info(f"Waiting {initial_wait:.1f} seconds before sending initial request...")
        time.sleep(initial_wait)
        
        # Dodajemo unique identifier
        request_id = str(uuid.uuid4())
        self.request_headers = {}  # Inicijaliziramo headers
        self.request_headers["X-Request-ID"] = request_id
        
        # Create report request
        report_request = self.prepare_request(context, None)
        response = self._request(
            prepared_request=report_request
        )
        report_info = response.json()
        
        logger.info("\n=== Initial Report Creation Response ===")
        logger.info(json.dumps(report_info, indent=2))
        logger.info("=== End Initial Report Creation Response ===\n")
        
        report_id = report_info["reportId"]
        max_attempts = 10  # Povećavamo broj pokušaja
        attempt = 0
        
        while attempt < max_attempts:
            # Dodajemo random jitter u vrijeme čekanja
            jitter = random.uniform(0, 5)
            wait_time = initial_wait * (2 ** attempt) + jitter
            
            logger.info(f"Waiting {wait_time} seconds before checking report status...")
            time.sleep(wait_time)
            
            report_status = self.get_report_status(report_id)
            
            if report_status["status"] == "COMPLETED":
                logger.info(f"Report completed! URL: {report_status['url']}")
                break
            elif report_status["status"] == "FAILED":
                logger.error(f"Report generation failed: {report_status.get('failureReason')}")
                break
            
            attempt += 1
            
        if attempt >= max_attempts:
            logger.warning(f"Reached maximum attempts waiting for report. Last status: {report_status['status']}")
        
        yield from self.process_report(report_info)

    def prepare_request(self, context: dict | None, next_page_token: t.Any | None) -> requests.PreparedRequest:
        """Prepare a request object."""
        logger.info("\n=== Preparing Request ===")
        
        http_method = self.method
        url = self.get_url(context)
        headers = self.http_headers
        
        from datetime import datetime
        
        start_date = self.config.get("start_date")
        if isinstance(start_date, datetime):
            start_date = start_date.strftime("%Y-%m-%d")
        elif not start_date:
            start_date = "2025-02-10"  # Default ako nema start_date
            
        end_date = self.config.get("end_date")
        if isinstance(end_date, datetime):
            end_date = end_date.strftime("%Y-%m-%d")
        elif not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        
        body = {
            "name": "SP Campaign Report",
            "startDate": start_date,
            "endDate": end_date,
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS",
                "groupBy": ["campaign","adGroup"],
                "columns": [
                    "campaignName",
                    "campaignId",
                    "adGroupName",
                    "adGroupId",
                    "adStatus",
                    "campaignStatus",
                    "campaignBudgetAmount",
                    "campaignBudgetType",
                    "campaignBudgetCurrencyCode",
                    "campaignBiddingStrategy",
                    "impressions",
                    "clicks",
                    "cost",
                    "costPerClick",
                    "clickThroughRate",
                    "purchases14d",
                    "sales14d",
                    "unitsSoldClicks14d",
                    "date"
                ],
                "reportTypeId": "spCampaigns",
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

    def get_records(self, context: dict | None) -> t.Iterable[dict]:
        """Get records from the source."""
        report_request = self.prepare_request(context, None)
        response = self._request(prepared_request=report_request)
        report_info = response.json()
        
        return self.process_report(report_info)
