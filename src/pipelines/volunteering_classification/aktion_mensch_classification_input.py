import dataclasses
from typing import Any, Optional

from src.pipelines.volunteering_classification.volunteering_classification_input import VolunteeringClassificationInput


@dataclasses.dataclass
class AktionMenschClassificationInput(VolunteeringClassificationInput):
    """Classification input for Aktion Mensch volunteering opportunities."""

    kurzbeschreibung: str = ""
    beschreibung: str = ""
    attribute: Any = None  # Store the raw attribute data for commitment label extraction
    verortung: Optional[str] = None  # Field for location type extraction
    source: Optional[str] = "Aktion Mensch"
    type: str = "aktion_mensch_volunteering"
