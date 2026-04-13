from dataclasses import dataclass
from urllib.parse import unquote


@dataclass
class SurveyPhoto(object):
    """
    Survey photo with its attribute
    """

    photo_id: str
    url: str = ""
    sta_meters: float = 0.0
    survey_year: int = 0
    linkid: str = ""
    latitude: float = 0.0
    longitude: float = 0.0

    def unquoted_url(self):
        """
        Return unquoted URL for comparison.
        """
        return unquote(self.url)
