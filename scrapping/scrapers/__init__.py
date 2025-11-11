"""Scraper modules for different data sources."""

from .api_scrapers import (
    HubeauQualiteRivieresAPI,
    HubeauHydrometrieAPI,
    HubeauQualiteNappesAPI,
)
from .geo_downloaders import GeoDownloader
from .base import BaseScraper, APIClient

__all__ = [
    'BaseScraper',
    'APIClient',
    'HubeauQualiteRivieresAPI',
    'HubeauHydrometrieAPI',
    'HubeauQualiteNappesAPI',
    'GeoDownloader',
]
