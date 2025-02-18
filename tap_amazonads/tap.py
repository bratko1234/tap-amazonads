"""AmazonADs tap class."""

from __future__ import annotations

from singer_sdk import Tap
from typing import List
from singer_sdk import typing as th

from tap_amazonads.streams import (
    CampaignsStream,
    AdGroupsStream,
    TargetsStream,
    AdsStream,
    AmazonADsStream,
    SearchTermReportStream,
    AdvertisedProductReportStream,
    PurchasedProductReportStream,
    GrossAndInvalidTrafficReportStream,
)
from tap_amazonads.config import CONFIG_SCHEMA

# Define which streams are available in the tap
STREAM_TYPES = [
    # Base streams
    CampaignsStream,
    AdGroupsStream,
    TargetsStream,
    AdsStream,
    # Report streams
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
            "enable_campaigns",
            th.BooleanType,
            default=True,
            description="Enable/disable campaigns stream",
        ),
        th.Property(
            "enable_ad_groups",
            th.BooleanType,
            default=True,
            description="Enable/disable ad groups stream",
        ),
        th.Property(
            "enable_targets",
            th.BooleanType,
            default=True,
            description="Enable/disable targets stream",
        ),
        th.Property(
            "enable_ads",
            th.BooleanType,
            default=True,
            description="Enable/disable ads stream",
        ),
        th.Property(
            "enable_search_term_reports",
            th.BooleanType,
            default=True,
            description="Enable/disable search term reports stream",
        ),
        th.Property(
            "enable_advertised_product_reports",
            th.BooleanType,
            default=True,
            description="Enable/disable advertised product reports stream",
        ),
        th.Property(
            "enable_purchased_product_reports",
            th.BooleanType,
            default=True,
            description="Enable/disable purchased product reports stream",
        ),
        th.Property(
            "enable_gross_and_invalid_traffic_reports",
            th.BooleanType,
            default=True,
            description="Enable/disable gross and invalid traffic reports stream",
        ),
    ).to_dict()

    def discover_streams(self) -> List[AmazonADsStream]:
        """Return a list of discovered streams."""
        enabled_streams = []
        
        for stream_type in STREAM_TYPES:
            stream_name = stream_type.__name__.lower().replace('stream', '')
            if self.config.get(f"enable_{stream_name}", True):
                enabled_streams.append(stream_type)
                
        return [stream_class(tap=self) for stream_class in enabled_streams]


if __name__ == "__main__":
    TapAmazonADs.cli()
