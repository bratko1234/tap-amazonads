"""AmazonADs tap class."""

from __future__ import annotations
import logging
from singer_sdk import Tap
from singer_sdk import typing as th
from typing import List

from tap_amazonads import streams

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
]

class TapAmazonADs(Tap):
    """AmazonADs tap class."""
    
    name = "tap-amazonads"

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
            description="The earliest record date to sync",
        ),
    ).to_dict()

    @property
    def selected_stream_names(self) -> list[str]:
        """Extract stream names from selection patterns."""
        if not self.selected:
            return []
        # Extract unique stream names (part before the dot)
        stream_names = {s.split('.')[0] for s in self.selected if '.' in s}
        logger.info(f"Extracted stream names from selection: {stream_names}")
        return list(stream_names)

    def discover_streams(self) -> List[streams.AmazonADsStream]:
        """Return a list of discovered streams."""
        all_streams = [stream_class(tap=self) for stream_class in STREAM_TYPES]
        
        selected_names = self.selected_stream_names
        if selected_names:
            logger.info(f"Filtering streams to: {selected_names}")
            return [s for s in all_streams if s.name in selected_names]
            
        logger.info("No stream selection - returning all streams")
        return all_streams

if __name__ == "__main__":
    TapAmazonADs.cli()
