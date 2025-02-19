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
        # Opcionalno: dodaj _select u JSON schemu ako želiš eksplicitno ga definisati
        th.Property(
            "_select",
            th.ArrayType(th.StringType),
            description="Selection extra for filtering streams",
            default=["*.*"],
        ),
    ).to_dict()

    @property
    def selected(self) -> list[str]:
        """Vraća listu selektovanih entiteta (iz _select extra)."""
        return self.config.get("_select", ["*.*"])

    @property
    def selected_streams(self) -> list[str]:
        """Izvlači imena odabranih streamova iz `selected`.

        Pretpostavljamo da su vrijednosti u formatu "stream_name.column_name".
        """
        if not self.selected:
            return []
        # Izvući jedinstvena imena streamova (deo pre tačke)
        return list({s.split('.')[0] for s in self.selected if '.' in s})

    def discover_streams(self) -> List[streams.AmazonADsStream]:
        all_streams = [stream_class(tap=self) for stream_class in STREAM_TYPES]
        # Ako postoji filtriranje streamova, vrati samo one čije ime je odabrano
        if self.selected_streams:
            return [s for s in all_streams if s.name in self.selected_streams]
        return all_streams

if __name__ == "__main__":
    TapAmazonADs.cli()
