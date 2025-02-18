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
        # Enable/disable streams
        th.Property(
            "enable_campaigns",
            th.BooleanType,
            default=True,
            description="Enable/disable campaigns stream",
        ),
        th.Property(
            "enable_adgroups",
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
        th.Property(
            "select",
            th.ArrayType(th.StringType),
            required=False,
            description="List of fields to sync (e.g. campaigns.campaignId)",
        ),
    ).to_dict()

    def discover_streams(self) -> List[streams.AmazonADsStream]:
        """Return a list of discovered streams."""
        enabled_streams = []
        streams_by_type = {}  # Dodajemo dictionary za praćenje stream-ova po tipu
        
        # Prvo kreiramo sve streamove
        for stream_type in STREAM_TYPES:
            stream_name = stream_type.__name__.lower().replace('stream', '')
            if self.config.get(f"enable_{stream_name}", False):
                stream = stream_type(tap=self)
                
                # Ako postoji select konfiguracija, primijeni je
                if "select" in self.config:
                    selected_fields = set()
                    for field in self.config["select"]:
                        if field.startswith(stream.name + "."):
                            field_path = field[len(stream.name) + 1:]
                            selected_fields.add(field_path)
                    
                    if selected_fields:
                        stream.selected_properties = selected_fields
                
                # Dodajemo stream u dictionary po tipu
                streams_by_type[stream_type] = stream
                
                # Ako stream nema parent_stream_type, dodajemo ga odmah u enabled_streams
                if not hasattr(stream_type, 'parent_stream_type') or stream_type.parent_stream_type is None:
                    enabled_streams.append(stream)
        
        # Zatim procesiramo streamove koji imaju parent_stream_type
        for stream_type in STREAM_TYPES:
            if hasattr(stream_type, 'parent_stream_type') and stream_type.parent_stream_type:
                stream = streams_by_type.get(stream_type)
                if stream:
                    # Provjeravamo da li je parent stream dostupan
                    parent_stream = streams_by_type.get(stream_type.parent_stream_type)
                    if parent_stream:
                        enabled_streams.append(stream)
                    else:
                        self.logger.warning(
                            f"Stream {stream_type.__name__} requires parent stream "
                            f"{stream_type.parent_stream_type.__name__} which is not enabled"
                        )
        
        return enabled_streams

if __name__ == "__main__":
    TapAmazonADs.cli()
