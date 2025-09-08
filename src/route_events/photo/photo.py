from dataclasses import dataclass
from urllib.parse import unquote

@dataclass
class SurveyPhoto(object):
    """
    Survey photo with its attribute
    """
    url: str
    sta_meters: float
    survey_year: int
    linkid: str
    latitude: float
    longitude: float

    def unquoted_url(self):
        """
        Return unquoted URL for comparison.
        """
        return unquote(self.url)