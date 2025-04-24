from route_events import RouteRNI, RouteRoughness
from ..analysis import segments_join
import polars as pl
from typing import Literal


class Kemantapan(object):
    """
    Route kemantapan object.
    """
    def __init__(
            self,
            iri: RouteRoughness,
            rni: RouteRNI
    ):
        self._iri = iri
        self._rni = rni
        
        # joined DataFrame
        self._joined = None

        # Grades
        self._kemantapan_grades = ['poor', 'bad', 'fair', 'good']

        # Default columns. Use the RNI columns
        self.linkid_col = rni._linkid_col
        self.to_sta_col = rni._to_sta_col
        self.from_sta_col = rni._from_sta_col
        self.surf_type_col = rni._surf_type_col

        # Other columns
        self.iri_col = iri._iri_col

    @property
    def joined(self):
        """
        Joined IRI-RNI DataFrame.
        """
        if self._joined is None:
            self._joined = segments_join(
                left = self._rni,
                right = self._iri,
                how='left',
                l_select = [self._rni._surf_type_col, self._rni._seg_len_col],
                r_select = [self._iri._iri_col],
                r_agg = [
                    # The IRI must be converted to Float32 first, if not then it will return null.
                    pl.col(self._iri._iri_col).cast(pl.Float32).mean()
                ],
                l_agg = [
                    # Just in case, also cast surface type as Int16
                    pl.col(self._rni._surf_type_col).cast(pl.Int16).max(),
                    pl.col(self._rni._seg_len_col).cast(pl.Float32).max()
                ]
            ).join(
                self._rni.surface_types_mapping,
                left_on=self._rni._surf_type_col,
                right_on='surf_type',
                how='left'
            )

            return self._joined
        
        else:
            return self._joined
        
    def grading_query(
            self, 
            summary_type:Literal[
                'iri_kemantapan', 
                'pci_kemantapan', 
                'iri_rating', 
                'pci_rating'
            ]
    ):
        """
        Grading query string for several summary types.
        """
        select_ = f"""
        select {self.linkid_col}, {self.from_sta_col}, {self.to_sta_col}
        """
        when_ = []
        cases_args = []
        grade_len = self.joined[summary_type][0].shape[0]

        for i in range(grade_len):
            if i == 0:
                when_.append(f'when {self.surf_type_col} in ({{}}) and {self.iri_col} <= {{}} then {i+1}')
                cases_args.append(pl.col(self.surf_type_col).cast(pl.List(pl.String)).list.join(', ')),
                cases_args.append(pl.col(summary_type).arr.get(i))
            
            elif i != grade_len-1:
                when_.append(f'when {self.surf_type_col} in ({{}}) and {self.iri_col} <= {{}} and {self.iri_col} > {{}} then {i+1}')
                cases_args.extend([
                    pl.col(self.surf_type_col).cast(pl.List(pl.String)).list.join(', '),
                    pl.col(summary_type).arr.get(i),
                    pl.col(summary_type).arr.get(i-1)
                ])

                when_.append(f'when {self.surf_type_col} in ({{}}) and {self.iri_col} <= {{}} and {self.iri_col} > {{}} then {i+2}')
                cases_args.extend([
                    pl.col(self.surf_type_col).cast(pl.List(pl.String)).list.join(', '),
                    pl.col(summary_type).arr.get(i+1),
                    pl.col(summary_type).arr.get(i)
                ])
            
            else:
                when_.append(f'when {self.surf_type_col} in ({{}}) and {self.iri_col} > {{}} then {i+2}')
                cases_args.append(pl.col(self.surf_type_col).cast(pl.List(pl.String)).list.join(', ')),
                cases_args.append(pl.col(summary_type).arr.get(i))

        case_ = self.joined.group_by(
            *[pl.col(summary_type).arr.get(_).alias('r'+str(_)) for _ in range(grade_len)]
        ).agg(
            pl.col(self.surf_type_col).unique()
        ).select(
            pl.col(self.surf_type_col),
            pl.concat_arr(*['r'+str(_) for _ in range(grade_len)]).alias(summary_type)
        ).with_columns(
            cases=pl.format(' '.join(when_), *cases_args)
        )

        from_ = " from joined"

        return select_ + ', case ' + ' '.join(case_['cases']) + ' else null end as grade' + from_
        
    def segment(
            self,
            summary_type:Literal[
                'iri_kemantapan',
                'pci_kemantapan',
                'iri_rating',
                'pci_rating'
            ],
            exclude_null_grade: bool = True,
            eager=True
        ):
        """
        Kemantapan grade for every segment.
        """
        ctxt = pl.SQLContext()
        ctxt.register('joined', self.joined)
        
        if exclude_null_grade:
            return ctxt.execute(
                self.grading_query(summary_type),
                eager=eager
            ).filter(
                pl.col('grade').is_not_null()
            )
        else:
            return ctxt.execute(
                self.grading_query(summary_type),
                eager=eager
            )
    
    def route(self, exclude_null_grade: bool = True):
        return