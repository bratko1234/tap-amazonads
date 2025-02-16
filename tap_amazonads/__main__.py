"""AmazonADs entry point."""

from __future__ import annotations

from tap_amazonads.tap import TapAmazonADs

TapAmazonADs.cli()
