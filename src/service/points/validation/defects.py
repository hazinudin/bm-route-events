from .base import RoutePointEventsValidation
from ...photo import gs
from route_events import (
    RouteDefects,
    RouteDefectsRepo,
    LRSRoute,
    SurveyPhoto
)
from ..analysis import segments_points_join
from sqlalchemy import Engine
from typing import List
from ...validation_result.result import ValidationResult
import polars as pl
from pydantic import ValidationError


class RouteDefectsValidation(RoutePointEventsValidation):
    """
    Route Defects Events validation class.
    """
    @classmethod
    def validate_excel(
        cls,
        excel_path: str,
        route: str,
        survey_year: int,
        sql_engine: Engine,
        lrs: LRSRoute,
        linkid_col: str = 'LINKID',
        ignore_review: bool = False,
        force_write: bool = False
    ):
        """
        Validate Defects data in Excel file.
        """
        if force_write:
            result = ValidationResult(route, ignore_in=['force'])
        elif force_write and ignore_review:
            result = ValidationResult(route, ignore_in=['force', 'review'])
        else:
            result = ValidationResult(route)

        obj = None

        try:
            events = RouteDefects.from_excel(
                excel_path=excel_path,
                linkid=route,
                linkid_col=linkid_col,
                ignore_review=ignore_review,
                data_year=survey_year
            )

            obj = cls(
                route=route,
                events=events,
                lrs=lrs,
                sql_engine=sql_engine,
                result=result,
                survey_year=survey_year
            )

            return obj
        
        except ValidationError as e:
            for error in e.errors():
                if 'review' in error['type']:
                    result.add_message(error['msg'], 'review', 'review')
                else:
                    result.add_message(error['msg'], 'rejected')
        
            if result.status == 'rejected':
                obj = cls(
                    route=route,
                    events=pl.DataFrame(),
                    lrs=None,
                    sql_engine=sql_engine,
                    result=result,
                    survey_year=survey_year
                )

                return obj
            
            else:
                events = RouteDefects.from_excel(
                    excel_path=excel_path,
                    linkid=route,
                    linkid_col=linkid_col,
                    ignore_review=True,
                    data_year=survey_year
                )

                obj = cls(
                    route=route,
                    events=events,
                    lrs=lrs,
                    sql_engine=sql_engine,
                    result=result,
                    survey_year=survey_year
                )

                return obj
        
        except IndexError:
            result.add_message(f"File '{excel_path}' tidak dapat ditemukan.", 'rejected')

            obj = cls(
                route=route,
                events=pl.DataFrame(),
                lrs=lrs,
                sql_engine=sql_engine,
                results=result,
                survey_year=survey_year
            )

            return obj

    def __init__(
            self,
            route: str,
            events: RouteDefects,
            lrs: LRSRoute,
            sql_engine: Engine,
            results: ValidationResult,
            photo_storage: gs.SurveyPhotoStorage,
            survey_year: int = None
    ):
        super().__init__(
            events=events,
            lrs=lrs,
            sql_engine=sql_engine,
            results=results,
            route=route,
            survey_year=survey_year
        )

        self._repo = RouteDefectsRepo(self._engine)

        # Survey photos
        self._photos = None

        # Photo storage
        self._storage = photo_storage
    
    @property
    def survey_photos(self) -> List[SurveyPhoto]:
        """
        Return a list containing SurveyPhoto with STA referenced to LRS.
        """
        if self._photos is None:
            self._photos = [
                SurveyPhoto(**_) for _ in self._events.pl_df.join(
                    self.df_lrs_mv,
                    on=[
                        self._events._linkid_col,
                        self._events._sta_col,
                        self._events._lane_code_col
                    ]
                ).select(
                    pl.when(
                        pl.col(self._events._photo_url_cols).str.to_lowercase().str.starts_with(self._storage.root_url)
                    ).then(
                        # Lowercased full URL
                        pl.col(self._events._photo_url_cols).str.to_lowercase()
                    ).otherwise(
                        pl.format(
                            # Filename must include /<province folder>/<route>/<file name>
                            "{}/{}/{}",
                            pl.lit(self._storage.root_url),
                            pl.col(self._events._year_col),
                            pl.col(self._events._photo_url_cols).str.to_lowercase()
                        )
                    ).alias('url'),
                    pl.col('m_val').alias('sta_meters'),
                    pl.lit(self._survey_year).alias('survey_year'),
                    pl.col(self._events._linkid_col).alias('linkid'),
                    pl.col(self._events._lat_col).alias('latitude'),
                    pl.col(self._events._long_col).alias('longitude')
                ).rows(named=True) 
            ]
            return self._photos
        else:
            return self._photos
        
    def survey_photo_url_check(self):
        """
        Validate photo URL or filename which is stored in the repository.
        """
        for invalid in self._storage.validate_photos_url(self.survey_photos, return_invalid=True):
            msg = f"{invalid.url} bukan merupakan URL valid atau gambar tidak ditemukan."

            self._result.add_message(
                msg,
                'error'
            )

        return

    def surface_type_check(self):
        """
        Compare defects surface type with surface type in RNI.
        """
        if self._events._surf_type_col == self.rni._surf_type_col:
            suffix = '_r'
        else:
            suffix = ''

        errors = segments_points_join(
            segments=self.rni,
            points=self._events,
            how='inner',
            point_select=[self._events._surf_type_col],
            segment_select=[self.rni._surf_type_col],
            suffix='_r'
        ).filter(
            pl.col(self._events._surf_type_col).ne(
                pl.col(self.rni._surf_type_col+suffix)
            )
        ).select(
            msg=pl.format(
                "Tipe perkerasan pada STA {} {} tidak sama dengan data RNI",
                pl.col(self._events._sta_col),
                pl.col(self._events._lane_code_col)
            )
        )

        self._result.add_messages(
            errors,
            'review',
            'review'
        )

        return
    
    def damage_surface_type_check(self):
        """
        This method will compare the damage type with the surface type in RNI.
        Rigid surface type is surface_type = 21.
        Damage that start with 'AS', should only exists on asphalt surface.
        Damage that start ith 'RG', should only exists on rigid surface.
        """
        from route_events.segments.rni.surf_type import _surface_types as surface_types

        df_surf_types = pl.DataFrame(surface_types)

        errors = self._events.pl_df.join(
            df_surf_types,
            left_on=[self._events._surf_type_col],
            right_on=['surf_type'],
            how='inner'
        ).filter(
            pl.col(self._events._defects_type_col).str.starts_with('AS').and_(
                pl.col('category').eq('paved')
            ).and_(
                pl.col(self._events._surf_type_col).eq(21)
            ) |
            pl.col(self._events._defects_type_col).str.starts_with('RG').and_(
                pl.col('category').eq('paved')
            ).and_(
                pl.col(self._events._surf_type_col).ne(21)
            ) |
            pl.col('category').eq('unpaved')
        ).select(
            msg=pl.format(
                "Tipe kerusakan {} pada STA {} {} tidak sesuai dengan tipe perkerasan",
                pl.col(self._events._defects_type_col),
                pl.col(self._events._sta_col),
                pl.col(self._events._lane_code_col)
            )
        )

        self._result.add_messages(
            errors,
            'review',
            'review'
        )

        return
    
    def put_data(self):
        """
        Delete and insert events data to geodatabase table.
        """
        self._repo.put(self._events, year=self._survey_year)

    def put_photos(self):
        """
        Put photo to photo database
        """
        self._storage.register_photos(
            photos=self.survey_photos,
            validate=False
        )
