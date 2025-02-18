"""AmazonADs tap class."""

from __future__ import annotations

from singer_sdk import Tap
from typing import List

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
    config_jsonschema = CONFIG_SCHEMA

    def discover_streams(self) -> List[AmazonADsStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapAmazonADs.cli()
