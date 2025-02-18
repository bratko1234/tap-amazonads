"""AmazonADs tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th
from typing import List

from tap_amazonads.streams import (
    CampaignsStream,
    AdGroupsStream,
    TargetsStream,
    AdsStream,
    SearchTermReportStream,
    AdvertisedProductReportStream,
    PurchasedProductReportStream,
    GrossAndInvalidTrafficReportStream,
)

STREAM_TYPES = [
    CampaignsStream,
    AdGroupsStream,
    TargetsStream,
    AdsStream,
    SearchTermReportStream,
    AdvertisedProductReportStream,
    PurchasedProductReportStream,
    GrossAndInvalidTrafficReportStream,
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
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        th.Property(
            "select",
            th.ArrayType(th.StringType),
            required=False,
            description="List of fields to sync (e.g. campaigns.campaignId)",
        ),
    ).to_dict()

    def setup_mapper(self):
        """Set up the stream mapper."""
        self._config.setdefault("flattening_enabled", True)
        self._config.setdefault("flattening_max_depth", 2)
        return super().setup_mapper()

    def discover_streams(self) -> List[streams.AmazonADsStream]:
        """Return a list of discovered streams."""
        enabled_streams = []
        
        for stream_type in STREAM_TYPES:
            stream_name = stream_type.__name__.lower().replace('stream', '')
            if self.config.get(f"enable_{stream_name}", True):
                enabled_streams.append(stream_type)
                
        return [stream_class(tap=self) for stream_class in enabled_streams]

if __name__ == "__main__":
    TapAmazonADs.cli()
