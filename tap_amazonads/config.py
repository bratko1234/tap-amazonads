"""AmazonADs Config."""

from singer_sdk import typing as th

CONFIG_SCHEMA = th.PropertiesList(
    th.Property(
        "client_id",
        th.StringType,
        required=True,
        secret=True,
        description="The client ID to authenticate against the Amazon Ads API service"
    ),
    th.Property(
        "client_secret",
        th.StringType,
        required=True,
        secret=True,
        description="The client secret to authenticate against the Amazon Ads API service"
    ),
    th.Property(
        "refresh_token",
        th.StringType,
        required=True,
        secret=True,
        description="The refresh token to authenticate against the Amazon Ads API service"
    ),
    th.Property(
        "profile_id",
        th.StringType,
        required=True,
        description="The Amazon Ads API profile ID"
    ),
    th.Property(
        "region",
        th.StringType,
        default="NA",
        description="The Amazon Ads API region (NA, EU, FE)",
        allowed_values=["NA", "EU", "FE"]
    ),
    th.Property(
        "start_date",
        th.DateTimeType,
        description="The earliest record date to sync"
    ),
    th.Property(
        "user_agent",
        th.StringType,
        default="tap-amazonads",
        description="User agent to present to the API"
    ),
    th.Property(
        "page_size",
        th.IntegerType,
        default=100,
        description="The number of records to fetch per request"
    ),
).to_dict() 