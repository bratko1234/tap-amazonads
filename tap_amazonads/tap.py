"""AmazonADs tap class."""

from __future__ import annotations
import logging
from singer_sdk import Tap
from singer_sdk import typing as th
from typing import List

from tap_amazonads import streams
from tap_amazonads.auth import AmazonADsAuthenticator

logger = logging.getLogger(__name__)

STREAM_TYPES = [
    streams.CampaignsStream,
    streams.AdGroupsStream,
    streams.TargetsStream,
    streams.AdsStream,
    streams.SearchTermReportStream,
    streams.AdvertisedProductReportStream,
    streams.PurchasedProductReportStream,
    streams.GrossAndInvalidTrafficReportStream,
    streams.CampaignReportStream,
]

class TapAmazonADs(Tap):
    """AmazonADs tap class."""
    
    name = "tap-amazonads"
    capabilities = ["catalog", "discover"]

    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            description="The client ID",
        ),
        th.Property(
            "client_secret", 
            th.StringType,
            required=True,
            secret=True,
            description="The client secret",
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            required=True,
            secret=True,
            description="The refresh token",
        ),
        th.Property(
            "profile_id",
            th.StringType,
            required=True,
            description="The Amazon Advertising API profile ID",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync (format: YYYY-MM-DD)",
        ),
    ).to_dict()

    @property
    def authenticator(self) -> AmazonADsAuthenticator:
        """Return a new authenticator."""
        logger.info("=== Getting tap authenticator ===")
        if not hasattr(self, '_authenticator'):
            logger.info("Creating new tap authenticator")
            self._authenticator = AmazonADsAuthenticator.create_for_stream(self)
            logger.info(f"Created new authenticator with access token (first 20 chars): {self._authenticator.access_token[:20] if hasattr(self._authenticator, 'access_token') else 'None'}")
        return self._authenticator

    def discover_streams(self) -> List[streams.AmazonADsStream]:
        """Return a list of discovered streams.
        
        Returns:
            A list of all available streams without filtering.
            Stream selection will be handled by Singer SDK based on metadata.
        """
        logger.info("Discovering all available streams")
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

if __name__ == "__main__":
    TapAmazonADs.cli()
