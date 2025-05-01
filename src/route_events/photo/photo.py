from dataclasses import dataclass

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