from dataclasses import dataclass

@dataclass
class SurveyPhoto(object):
    url: str
    sta: float
    survey_year: int
    linkid: str
    latitude: float
    longitude: float