"""Pydantic models for data validation.

Models correspond to the schemas defined in the playbook:
- qualite_analyse
- hydrometrie_obs_elab
- roe_obstacle
- topage_troncon
- commune
- pdf_index
"""

from datetime import date, datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator


class QualiteAnalyse(BaseModel):
    """Schema for water quality analysis data."""

    code_station: str = Field(..., description="Station code")
    libelle_station: str = Field(..., description="Station name")
    code_commune: Optional[str] = Field(None, description="Municipality code")
    code_parametre: str = Field(..., description="Parameter code")
    libelle_parametre: str = Field(..., description="Parameter name")
    fraction_analysee: Optional[str] = Field(None, description="Analyzed fraction")
    resultat: Optional[float] = Field(None, description="Analysis result")
    unite: Optional[str] = Field(None, description="Unit of measurement")
    date_prelevement: date = Field(..., description="Sampling date")
    code_masse_eau: Optional[str] = Field(None, description="Water body code")

    class Config:
        json_schema_extra = {
            "example": {
                "code_station": "04127000",
                "libelle_station": "LA LOIRE A MONTJEAN-SUR-LOIRE",
                "code_commune": "49207",
                "code_parametre": "1340",
                "libelle_parametre": "Nitrates",
                "fraction_analysee": "23",
                "resultat": 15.5,
                "unite": "mg/L",
                "date_prelevement": "2023-06-15",
                "code_masse_eau": "FRGR0372"
            }
        }


class HydrometrieObsElab(BaseModel):
    """Schema for hydrometric observations (elaborated)."""

    code_site: str = Field(..., description="Site code")
    code_station: str = Field(..., description="Station code")
    grandeur_hydro: str = Field(..., description="Hydrological variable (QmM, QmnJ, etc.)")
    date_obs: date = Field(..., description="Observation date")
    resultat: Optional[float] = Field(None, description="Observation value")
    unite: Optional[str] = Field(None, description="Unit of measurement")

    class Config:
        json_schema_extra = {
            "example": {
                "code_site": "K4370010",
                "code_station": "K437001001",
                "grandeur_hydro": "QmM",
                "date_obs": "2023-06-01",
                "resultat": 125.5,
                "unite": "m3/s"
            }
        }


class ROEObstacle(BaseModel):
    """Schema for river obstacles (ROE)."""

    id_ouvrage: str = Field(..., description="Obstacle/structure ID")
    type_ouvrage: Optional[str] = Field(None, description="Structure type")
    geometry: Dict[str, Any] = Field(..., description="GeoJSON geometry")
    nom_cours_eau: Optional[str] = Field(None, description="River name")

    class Config:
        json_schema_extra = {
            "example": {
                "id_ouvrage": "ROE12345",
                "type_ouvrage": "Barrage",
                "geometry": {
                    "type": "Point",
                    "coordinates": [-0.5545, 47.3833]
                },
                "nom_cours_eau": "La Loire"
            }
        }


class TopageTroncon(BaseModel):
    """Schema for hydrographic segments (BD TOPAGE)."""

    id_troncon: str = Field(..., description="Segment ID")
    geometry: Dict[str, Any] = Field(..., description="GeoJSON geometry")
    classif: Optional[str] = Field(None, description="Classification")

    class Config:
        json_schema_extra = {
            "example": {
                "id_troncon": "TRONC123456",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [[-0.55, 47.38], [-0.54, 47.39]]
                },
                "classif": "Cours d'eau"
            }
        }


class Commune(BaseModel):
    """Schema for municipality data."""

    code_insee: str = Field(..., description="INSEE code")
    nom: str = Field(..., description="Municipality name")
    geometry: Dict[str, Any] = Field(..., description="GeoJSON geometry")

    class Config:
        json_schema_extra = {
            "example": {
                "code_insee": "49207",
                "nom": "Montjean-sur-Loire",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-0.86, 47.38], [-0.85, 47.39], [-0.84, 47.38], [-0.86, 47.38]]]
                }
            }
        }


class PDFIndex(BaseModel):
    """Schema for PDF document index."""

    title: str = Field(..., description="Document title")
    year: Optional[int] = Field(None, description="Publication year")
    source: str = Field(..., description="Source identifier")
    url: str = Field(..., description="Original URL")
    local_path: str = Field(..., description="Local file path")
    hash: str = Field(..., description="File hash (SHA256)")
    downloaded_at: Optional[datetime] = Field(None, description="Download timestamp")

    @field_validator('year')
    @classmethod
    def validate_year(cls, v: Optional[int]) -> Optional[int]:
        """Validate year is reasonable."""
        if v is not None and (v < 1900 or v > 2100):
            raise ValueError(f"Year {v} is out of reasonable range")
        return v

    class Config:
        json_schema_extra = {
            "example": {
                "title": "Bulletin de Situation Hydrologique - Juin 2023",
                "year": 2023,
                "source": "bsh_loire_bretagne",
                "url": "https://example.com/bsh_2023_06.pdf",
                "local_path": "raw/pdfs/bsh/bsh_2023_06.pdf",
                "hash": "abc123def456...",
                "downloaded_at": "2024-01-15T10:30:00Z"
            }
        }


# Validation functions

def validate_qualite_analyses(data: list) -> list:
    """Validate list of quality analysis records.

    Args:
        data: List of dicts

    Returns:
        List of validated QualiteAnalyse objects

    Raises:
        ValidationError if any record is invalid
    """
    return [QualiteAnalyse(**record) for record in data]


def validate_hydrometrie_obs(data: list) -> list:
    """Validate list of hydrometric observation records.

    Args:
        data: List of dicts

    Returns:
        List of validated HydrometrieObsElab objects

    Raises:
        ValidationError if any record is invalid
    """
    return [HydrometrieObsElab(**record) for record in data]


def validate_roe_obstacles(data: list) -> list:
    """Validate list of ROE obstacle records.

    Args:
        data: List of dicts

    Returns:
        List of validated ROEObstacle objects

    Raises:
        ValidationError if any record is invalid
    """
    return [ROEObstacle(**record) for record in data]


def validate_topage_troncons(data: list) -> list:
    """Validate list of TOPAGE segment records.

    Args:
        data: List of dicts

    Returns:
        List of validated TopageTroncon objects

    Raises:
        ValidationError if any record is invalid
    """
    return [TopageTroncon(**record) for record in data]


def validate_communes(data: list) -> list:
    """Validate list of commune records.

    Args:
        data: List of dicts

    Returns:
        List of validated Commune objects

    Raises:
        ValidationError if any record is invalid
    """
    return [Commune(**record) for record in data]


def validate_pdf_indices(data: list) -> list:
    """Validate list of PDF index records.

    Args:
        data: List of dicts

    Returns:
        List of validated PDFIndex objects

    Raises:
        ValidationError if any record is invalid
    """
    return [PDFIndex(**record) for record in data]
