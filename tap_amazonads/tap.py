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
)
from tap_amazonads.config import CONFIG_SCHEMA


class TapAmazonADs(Tap):
    """AmazonADs tap class."""

    name = "tap-amazonads"
    config_jsonschema = CONFIG_SCHEMA

    def discover_streams(self) -> List[AmazonADsStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            CampaignsStream(self),
            AdGroupsStream(self),
            TargetsStream(self),
            AdsStream(self),
        ]


if __name__ == "__main__":
    TapAmazonADs.cli()
