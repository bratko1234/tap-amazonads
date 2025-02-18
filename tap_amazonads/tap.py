"""AmazonADs tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th
from typing import List

from tap_amazonads import streams

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
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
    ).to_dict()

    def discover_streams(self) -> List[streams.AmazonADsStream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

if __name__ == "__main__":
    TapAmazonADs.cli()
