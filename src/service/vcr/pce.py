from .pipeline import MultiDataContext, PipelineContext, PipelineStep
from .expr import CapacityExpressions
from copy import deepcopy
from typing import Literal
import polars as pl


class VolumePCELookup(PipelineStep):
    def __init__(self):
        super().__init__('PCE_lookup')

    def execute(self, ctx: MultiDataContext) -> PipelineContext:
        """
        PCE lookup depends on segment attribute.
        """
        if (
            'RNI' not in MultiDataContext.datas.keys()
        ) or (
            'VOLH' not in MultiDataContext.datas.keys()
        ):
            raise KeyError('Context does not contain RNI or VOLH dataset')
        
        expr = CapacityExpressions(ctx)
        vol_o_col = 'TOTAL_VOL_O'
        vol_n_col = 'TOTAL_VOL_N'
        total_vol_col = 'TOTAL_VOL'

        # Volume pivot with direction as linkid as index
        volh_pivot = ctx.datas['VOLH'].with_columns(
            TOTAL_VOL=pl.sum_horizontal(
                [ctx.veh1_col, ctx.veh2_col, ctx.veh3_col, ctx.veh4_col, ctx.veh5_col]
            )
        ).group_by(
            ctx.linkid_col, ctx.survey_date_col, ctx.survey_hours_col
        ).agg(
            pl.col(
                _col
            ).filter(
                pl.col(ctx.dir_col).eq(direc)
            ).sum().alias(
                f"{_col}_{direc}"
            ) for direc in ['O', 'N'] for _col in [
                total_vol_col, ctx.veh1_col, ctx.veh2_col, ctx.veh3_col, ctx.veh4_col, ctx.veh5_col
            ]
        ).with_columns(
            TOTAL_VOL=pl.col(vol_o_col).add(pl.col(vol_n_col))
        )

        # Joined RNI-Volume LazyFrame
        joined = ctx.datas['RNI'].join(
            volh_pivot,
            on=ctx.linkid_col
        )

        # Total volume range expression
        def volume_range(lower: int | None = None, upper: int | None = None, col: str = 'TOTAL_VOL') -> pl.Expr:
            if (upper is None) and (lower is None):
                raise ValueError("upper and lower is None.")
            
            if upper is None:
                return pl.col(col).ge(lower)
            
            if lower is None:
                return pl.col(col).le(upper)
            
            return pl.col(col).ge(
                lower
            ).le(
                upper
            )
        
        # Lane width range expression
        def lane_range(lower: int | None = None, upper: int | None = None) -> pl.Expr:
            if (upper is None) and (lower is None):
                raise ValueError("upper and lower is None.")

            if upper is None:
                return pl.col(ctx.total_lanew_col).gt(lower)
            
            if lower is None:
                return pl.col(ctx.total_lanew_col).lt(upper)
            
            return pl.col(ctx.total_lanew_col).ge(
                lower
            ).le(
                upper
            )

        # Road stat = LK
        # Luar kota
        lk_lf = joined.filter(
            expr.lk_stat()
        ).with_columns(
            # For undivided two way OR one way
            PCE=pl.when(
                # Flat terrain
                # Undivided
                # 3 lanes or less
                # Table 3-9 page 71
                expr.flat_terrain().and_(
                    expr.undivided()
                ).and_(
                    expr.lane_count(3, 'le')
                ).and_(
                    expr.two_way()
                )
            ).then(
                pl.when(
                    # Condition
                    # Volume between 0-799
                    # Lane width for motorcycle is < 6
                    volume_range(0, 799).and_(
                        lane_range(upper=6)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.2,
                            "emp_bb": 1.2,
                            "emp_tb": 1.8,
                            "emp_sm": 0.8
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 0-799
                    # Lane width for motorcycle is between 6-8
                    volume_range(0, 799).and_(
                        lane_range(6, 8)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.2,
                            "emp_bb": 1.2,
                            "emp_tb": 1.8,
                            "emp_sm": 0.6
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 0-799
                    # Lane width for motorcycle greater than 8
                    volume_range(0, 799).and_(
                        lane_range(lower=8)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.2,
                            "emp_bb": 1.2,
                            "emp_tb": 1.8,
                            "emp_sm": 0.4
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 800-1349
                    # Lane width for motorcycle is < 6
                    volume_range(800, 1349).and_(
                        lane_range(upper=6)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.8,
                            "emp_bb": 1.8,
                            "emp_tb": 2.7,
                            "emp_sm": 1.2
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 800-1349
                    # Lane width for motorcycle is between 6-8
                    volume_range(800, 1349).and_(
                        lane_range(6, 8)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.8,
                            "emp_bb": 1.8,
                            "emp_tb": 2.7,
                            "emp_sm": 0.9
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 800-1349
                    # Lane width for motorcycle  is greater than 8
                    volume_range(800, 1349).and_(
                        lane_range(lower=8)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.8,
                            "emp_bb": 1.8,
                            "emp_tb": 2.7,
                            "emp_sm": 0.6
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 800-1349
                    # Lane width for motorcycle is < 6
                    volume_range(1350, 1899).and_(
                        lane_range(upper=6)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.5,
                            "emp_bb": 1.6,
                            "emp_tb": 2.5,
                            "emp_sm": 0.9
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 800-1349
                    # Lane width for motorcycle is between 6-8
                    volume_range(800, 1349).and_(
                        lane_range(6, 8)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.5,
                            "emp_bb": 1.6,
                            "emp_tb": 2.5,
                            "emp_sm": 0.7
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 800-1349
                    # Lane width for motorcycle is greater than 8
                    volume_range(800, 1349).and_(
                        lane_range(lower=8)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.5,
                            "emp_bb": 1.6,
                            "emp_tb": 2.5,
                            "emp_sm": 0.5
                        }
                    )
                ).when(
                    # Condition
                    # Volume greater or equal to 1900
                    # Lane width for motorcycle is < 6
                    volume_range(lower=1900).and_(
                        lane_range(upper=6)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.3,
                            "emp_bb": 1.5,
                            "emp_tb": 2.5,
                            "emp_sm": 0.6
                        }
                    )
                ).when(
                    # Condition
                    # Volume greater or equal to 1900
                    # Lane width for motorcycle is between 6-8
                    volume_range(lower=1900).and_(
                        lane_range(6, 8)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.3,
                            "emp_bb": 1.5,
                            "emp_tb": 2.5,
                            "emp_sm": 0.5
                        }
                    )
                ).when(
                    # Condition
                    # Volume greater or equal to 1900
                    # Lane width for motorcycle is greater than 8
                    volume_range(lower=1900).and_(
                        lane_range(lower=8)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.3,
                            "emp_bb": 1.5,
                            "emp_tb": 2.5,
                            "emp_sm": 0.4
                        }
                    )
                )
            ).when(
                # Hilly terrain
                # Undivided
                # 3 lanes or less
                # Table 3-9 page 71
                expr.hilly_terrain().and_(
                    expr.undivided()
                ).and_(
                    expr.lane_count(3, 'le')
                ).and_(
                    expr.two_way()
                )
            ).then(
                pl.when(
                    # Condition
                    # Volume between 0-649
                    # Lane width for motorcycle is less than 6
                    volume_range(0, 649).and_(
                        lane_range(upper=6)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.8,
                            "emp_bb": 1.6,
                            "emp_tb": 5.2,
                            "emp_sm": 0.7
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 0-649
                    # Lane width for motorcycle is between 6-8
                    volume_range(0, 649).and_(
                        lane_range(6, 8)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.8,
                            "emp_bb": 1.6,
                            "emp_tb": 5.2,
                            "emp_sm": 0.5
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 0-649
                    # Lane width for motorcycle is greater than 8
                    volume_range(0, 649).and_(
                        lane_range(lower=8)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.8,
                            "emp_bb": 1.6,
                            "emp_tb": 5.2,
                            "emp_sm": 0.3
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 650-1099
                    # Lane width for motorcycle is less than 6
                    volume_range(650, 1099).and_(
                        lane_range(upper=6)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.4,
                            "emp_bb": 2.5,
                            "emp_tb": 5.0,
                            "emp_sm": 1.0
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 650-1099
                    # Lane width for motorcycle is between 6-8
                    volume_range(650, 1099).and_(
                        lane_range(6, 8)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.4,
                            "emp_bb": 2.5,
                            "emp_tb": 5.0,
                            "emp_sm": 0.8
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 650-1099
                    # Lane width for motorcycle is greater than 8
                    volume_range(650, 1099).and_(
                        lane_range(lower=8)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.4,
                            "emp_bb": 2.5,
                            "emp_tb": 5.0,
                            "emp_sm": 0.5
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 1100-1599
                    # Lane width for motorcycle is less than 6
                    volume_range(1100, 1599).and_(
                        lane_range(upper=6)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.0,
                            "emp_bb": 2.0,
                            "emp_tb": 4.0,
                            "emp_sm": 0.8
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 1100-1599
                    # Lane width for motorcycle is between 6-8
                    volume_range(1100, 1599).and_(
                        lane_range(6, 8)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.0,
                            "emp_bb": 2.0,
                            "emp_tb": 4.0,
                            "emp_sm": 0.6
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 1100-1599
                    # Lane width for motorcycle is greater than 8
                    volume_range(1100, 1599).and_(
                        lane_range(lower=8)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.0,
                            "emp_bb": 2.0,
                            "emp_tb": 4.0,
                            "emp_sm": 0.4
                        }
                    )
                ).when(
                    # Condition
                    # Volume greater than 1600
                    # Lane width for motorcycle is less than 6
                    volume_range(lower=1600).and_(
                        lane_range(upper=6)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.7,
                            "emp_bb": 1.7,
                            "emp_tb": 3.2,
                            "emp_sm": 0.5
                        }
                    )
                ).when(
                    # Condition
                    # Volume greater than 1600
                    # Lane width for motorcycle is between 6-8
                    volume_range(lower=1600).and_(
                        lane_range(6, 8)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.7,
                            "emp_bb": 1.7,
                            "emp_tb": 3.2,
                            "emp_sm": 0.4
                        }
                    )
                ).when(
                    # Condition
                    # Volume greater than 1600
                    # Lane width for motorcycle is greater than 8
                    volume_range(lower=1600).and_(
                        lane_range(lower=8)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.7,
                            "emp_bb": 1.7,
                            "emp_tb": 3.2,
                            "emp_sm": 0.3
                        }
                    )
                )
            ).when(
                # Mountainous terrain
                # Undivided
                # 3 lanes or less
                # Table 3-9 page 71
                expr.moutainous_terrain().and_(
                    expr.undivided()
                ).and_(
                    expr.lane_count(3, 'le')
                ).and_(
                    expr.two_way()
                )
            ).then(
                pl.when(
                    # Condition
                    # Volume between 0-649
                    # Lane width for motorcycle is less than 6
                    volume_range(0, 449).and_(
                        lane_range(upper=6)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 3.5,
                            "emp_bb": 2.5,
                            "emp_tb": 6.0,
                            "emp_sm": 0.6
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 0-649
                    # Lane width for motorcycle is between 6-8
                    volume_range(0, 449).and_(
                        lane_range(6, 8)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 3.5,
                            "emp_bb": 2.5,
                            "emp_tb": 6.0,
                            "emp_sm": 0.4
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 0-649
                    # Lane width for motorcycle is greater than 8
                    volume_range(0, 449).and_(
                        lane_range(lower=8)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 3.5,
                            "emp_bb": 2.5,
                            "emp_tb": 6.0,
                            "emp_sm": 0.2
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 450-899
                    # Lane width for motorcycle is less than 6
                    volume_range(450, 899).and_(
                        lane_range(upper=6)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 3.0,
                            "emp_bb": 3.2,
                            "emp_tb": 5.5,
                            "emp_sm": 0.9
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 450-899
                    # Lane width for motorcycle is between 6-8
                    volume_range(450, 899).and_(
                        lane_range(6, 8)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 3.0,
                            "emp_bb": 3.2,
                            "emp_tb": 5.5,
                            "emp_sm": 0.7
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 450-899
                    # Lane width for motorcycle is greater than 8
                    volume_range(450, 899).and_(
                        lane_range(lower=8)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 3.0,
                            "emp_bb": 3.2,
                            "emp_tb": 5.5,
                            "emp_sm": 0.4
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 900-1349
                    # Lane width for motorcycle is less than 6
                    volume_range(900, 1349).and_(
                        lane_range(upper=6)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.5,
                            "emp_bb": 2.5,
                            "emp_tb": 5.0,
                            "emp_sm": 0.7
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 900-1349
                    # Lane width for motorcycle is between 6-8
                    volume_range(900, 1349).and_(
                        lane_range(6, 8)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.5,
                            "emp_bb": 2.5,
                            "emp_tb": 5.0,
                            "emp_sm": 0.5
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 900-1349
                    # Lane width for motorcycle is greater than 8
                    volume_range(900, 1349).and_(
                        lane_range(lower=8)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.5,
                            "emp_bb": 2.5,
                            "emp_tb": 5.0,
                            "emp_sm": 0.3
                        }
                    )
                ).when(
                    # Condition
                    # Volume greater than 1350
                    # Lane width for motorcycle is less than 6
                    volume_range(lower=1350).and_(
                        lane_range(upper=6)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.9,
                            "emp_bb": 2.2,
                            "emp_tb": 4.0,
                            "emp_sm": 0.5
                        }
                    )
                ).when(
                    # Condition
                    # Volume greater than 1350
                    # Lane width for motorcycle is between 6-8
                    volume_range(lower=1350).and_(
                        lane_range(6, 8)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.9,
                            "emp_bb": 2.2,
                            "emp_tb": 4.0,
                            "emp_sm": 0.4
                        }
                    )
                ).when(
                    # Condition
                    # Volume greater than 1350
                    # Lane width for motorcycle is greater than 8
                    volume_range(lower=1350).and_(
                        lane_range(lower=8)
                    )
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.9,
                            "emp_bb": 2.2,
                            "emp_tb": 4.0,
                            "emp_sm": 0.3
                        }
                    )
                )
            ).when(
                # Flat terrain
                # Undivided
                # 4 lanes or more
                # EMP value from 3-10
                # Custom volume range, not in manual
                expr.flat_terrain().and_(
                    expr.undivided()
                ).and_(
                    expr.lane_count(4, 'ge')
                ).and_(
                    expr.two_way()
                )
            ).then(
                pl.when(
                    # Condition
                    # Volume between 0-1699
                    volume_range(0, 1699)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.2,
                            "emp_bb": 1.2,
                            "emp_tb": 1.6,
                            "emp_sm": 0.5
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 1700-3249
                    volume_range(1700, 3249)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.4,
                            "emp_bb": 1.4,
                            "emp_tb": 2.0,
                            "emp_sm": 0.6
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 3250-3949
                    volume_range(3250, 3949)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.6,
                            "emp_bb": 1.7,
                            "emp_tb": 2.5,
                            "emp_sm": 0.8
                        }
                    )
                ).when(
                    # Condition
                    # Volume greater than 3950
                    volume_range(lower=3950)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.3,
                            "emp_bb": 1.5,
                            "emp_tb": 2.0,
                            "emp_sm": 0.5
                        }
                    )
                )
            ).when(
                # Hilly terrain
                # Undivided
                # 4 lanes or more
                # EMP value from 3-10
                # Custom volume range, not in manual
                expr.hilly_terrain().and_(
                    expr.undivided()
                ).and_(
                    expr.lane_count(4, 'ge')
                ).and_(
                    expr.two_way()
                )
            ).then(
                pl.when(
                    # Condition
                    # Volume between 0-1349
                    volume_range(0, 1349)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.8,
                            "emp_bb": 1.6,
                            "emp_tb": 4.8,
                            "emp_sm": 0.4
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 1350-2499
                    volume_range(1350, 2499)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.0,
                            "emp_bb": 2.0,
                            "emp_tb": 4.6,
                            "emp_sm": 0.5
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 2500-3149
                    volume_range(2500, 3149)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.2,
                            "emp_bb": 2.3,
                            "emp_tb": 4.3,
                            "emp_sm": 0.7
                        }
                    )
                ).when(
                    # Condition
                    # Volume greater than 3150
                    volume_range(lower=3150)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.8,
                            "emp_bb": 1.9,
                            "emp_tb": 3.5,
                            "emp_sm": 0.4
                        }
                    )
                )
            ).when(
                # Mountainous terrain
                # Undivided
                # 4 lanes or more
                # EMP value from 3-10
                # Custom volume range, not in manual
                expr.moutainous_terrain().and_(
                    expr.undivided()
                ).and_(
                    expr.lane_count(4, 'ge')
                ).and_(
                    expr.two_way()
                )
            ).then(
                pl.when(
                    # Condition
                    # Volume between 0-999
                    volume_range(0, 999)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 3.2,
                            "emp_bb": 2.2,
                            "emp_tb": 5.5,
                            "emp_sm": 0.3
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 1000-1999
                    volume_range(1000, 1999)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.9,
                            "emp_bb": 2.6,
                            "emp_tb": 5.1,
                            "emp_sm": 0.4
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 2000-2699
                    volume_range(2000, 2699)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.6,
                            "emp_bb": 2.9,
                            "emp_tb": 4.8,
                            "emp_sm": 0.6
                        }
                    )
                ).when(
                    # Condition
                    # Volume greater than 2700
                    volume_range(lower=2700)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.0,
                            "emp_bb": 2.4,
                            "emp_tb": 3.8,
                            "emp_sm": 0.3
                        }
                    )
                )
            ).when(
                # Flat terrain
                # One way
                # Table 3-10
                expr.flat_terrain().and_(
                    expr.one_way()
                )
            ).then(
                pl.when(
                    # Condition
                    # Volume between 0-999
                    volume_range(0, 999)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.2,
                            "emp_bb": 1.2,
                            "emp_tb": 1.6,
                            "emp_sm": 0.5
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 1000-1799
                    volume_range(1000, 1799)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.4,
                            "emp_bb": 1.4,
                            "emp_tb": 2.0,
                            "emp_sm": 0.6
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 1800-2149
                    volume_range(1800, 2149)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.6,
                            "emp_bb": 1.7,
                            "emp_tb": 2.5,
                            "emp_sm": 0.8
                        }
                    )
                ).when(
                    # Condition
                    # Volume greater than 2150
                    volume_range(lower=2150)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.3,
                            "emp_bb": 1.5,
                            "emp_tb": 2.0,
                            "emp_sm": 0.5
                        }
                    )
                )
            ).when(
                # Hilly terrain
                # One way
                # Table 3-10
                expr.hilly_terrain().and_(
                    expr.one_way()
                )
            ).then(
                pl.when(
                    # Condition
                    # Volume between 0-749
                    volume_range(0, 749)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.8,
                            "emp_bb": 1.6,
                            "emp_tb": 4.8,
                            "emp_sm": 0.4
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 750-1399
                    volume_range(750, 1399)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.0,
                            "emp_bb": 2.0,
                            "emp_tb": 4.6,
                            "emp_sm": 0.5
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 1400-1749
                    volume_range(1400, 1749)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.2,
                            "emp_bb": 2.3,
                            "emp_tb": 4.3,
                            "emp_sm": 0.7
                        }
                    )
                ).when(
                    # Condition
                    # Volume greater than 1750
                    volume_range(lower=1750)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.8,
                            "emp_bb": 1.9,
                            "emp_tb": 3.5,
                            "emp_sm": 0.4
                        }
                    )
                )
            ).when(
                # Mountainous terrain
                # One way
                # Table 3-10
                expr.moutainous_terrain().and_(
                    expr.one_way()
                )
            ).then(
                pl.when(
                    # Condition
                    # Volume between 0-549
                    volume_range(0, 549)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 3.2,
                            "emp_bb": 2.2,
                            "emp_tb": 5.5,
                            "emp_sm": 0.3
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 550-1099
                    volume_range(550, 1099)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.9,
                            "emp_bb": 2.6,
                            "emp_tb": 5.1,
                            "emp_sm": 0.4
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 1100-1499
                    volume_range(1800, 2149)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.6,
                            "emp_bb": 2.9,
                            "emp_tb": 4.8,
                            "emp_sm": 0.6
                        }
                    )
                ).when(
                    # Condition
                    # Volume greater than 1500
                    volume_range(lower=1500)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.0,
                            "emp_bb": 2.4,
                            "emp_tb": 3.8,
                            "emp_sm": 0.3
                        }
                    )
                )
            ),

            # For two way divided
            PCE_N=pl.when(
                # Flat terrain
                # Divided
                # Two way
                # 5 lanes or less
                # Table 3-10
                expr.flat_terrain().and_(
                    expr.divided()
                ).and_(
                    expr.lane_count(5, 'le')
                )
            ).then(
                pl.when(
                    # Condition
                    # Volume between 0-999
                    volume_range(0, 999, vol_n_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.2,
                            "emp_bb": 1.2,
                            "emp_tb": 1.6,
                            "emp_sm": 0.5
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 1000-1799
                    volume_range(1000, 1799, vol_n_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.4,
                            "emp_bb": 1.4,
                            "emp_tb": 2.0,
                            "emp_sm": 0.6
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 1800-2149
                    volume_range(1800, 2149, vol_n_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.6,
                            "emp_bb": 1.7,
                            "emp_tb": 2.5,
                            "emp_sm": 0.8
                        }
                    )
                ).when(
                    # Condition
                    # Volume greater than 2150
                    volume_range(lower=2150, col=vol_n_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.3,
                            "emp_bb": 1.5,
                            "emp_tb": 2.0,
                            "emp_sm": 0.5
                        }
                    )
                )
            ).when(
                # Hilly terrain
                # Divided
                # Two way
                # 5 lanes or less
                # Table 3-10
                expr.hilly_terrain().and_(
                    expr.divided()
                ).and_(
                    expr.lane_count(5, 'le')
                )
            ).then(
                pl.when(
                    # Condition
                    # Volume between 0-749
                    volume_range(0, 749, vol_n_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.8,
                            "emp_bb": 1.6,
                            "emp_tb": 4.8,
                            "emp_sm": 0.4
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 750-1399
                    volume_range(750, 1399, vol_n_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.0,
                            "emp_bb": 2.0,
                            "emp_tb": 4.6,
                            "emp_sm": 0.5
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 1400-1749
                    volume_range(1400, 1749, vol_n_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.2,
                            "emp_bb": 2.3,
                            "emp_tb": 4.3,
                            "emp_sm": 0.7
                        }
                    )
                ).when(
                    # Condition
                    # Volume greater than 1750
                    volume_range(lower=1750, col=vol_n_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.8,
                            "emp_bb": 1.9,
                            "emp_tb": 3.5,
                            "emp_sm": 0.4
                        }
                    )
                )
            ).when(
                # Mountainous terrain
                # Divided
                # Two way
                # 5 lanes or less
                # Table 3-10
                expr.moutainous_terrain().and_(
                    expr.divided()
                ).and_(
                    expr.lane_count(5, 'le')
                )
            ).then(
                pl.when(
                    # Condition
                    # Volume between 0-549
                    volume_range(0, 549, vol_n_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 3.2,
                            "emp_bb": 2.2,
                            "emp_tb": 5.5,
                            "emp_sm": 0.3
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 550-1099
                    volume_range(550, 1099, vol_n_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.9,
                            "emp_bb": 2.6,
                            "emp_tb": 5.1,
                            "emp_sm": 0.4
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 1100-1499
                    volume_range(1800, 2149, vol_n_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.6,
                            "emp_bb": 2.9,
                            "emp_tb": 4.8,
                            "emp_sm": 0.6
                        }
                    )
                ).when(
                    # Condition
                    # Volume greater than 1500
                    volume_range(lower=1500, col=vol_n_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.0,
                            "emp_bb": 2.4,
                            "emp_tb": 3.8,
                            "emp_sm": 0.3
                        }
                    )
                )
            ).when(
                # Flat terrain
                # Divided
                # Two way
                # 6 lanes or more
                # Table 3-11
                expr.flat_terrain().and_(
                    expr.divided()
                ).and_(
                    expr.lane_count(6, 'ge')
                )
            ).then(
                pl.when(
                    # Condition
                    # Volume between 0-1499
                    volume_range(0, 1499, vol_n_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.2,
                            "emp_bb": 1.2,
                            "emp_tb": 1.6,
                            "emp_sm": 0.5
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 1500-2749
                    volume_range(1500, 2749, vol_n_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.4,
                            "emp_bb": 1.4,
                            "emp_tb": 2.0,
                            "emp_sm": 0.6
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 2750-3249
                    volume_range(2750, 3249, vol_n_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.6,
                            "emp_bb": 1.7,
                            "emp_tb": 2.5,
                            "emp_sm": 0.8
                        }
                    )
                ).when(
                    # Condition
                    # Volume greater than 3250
                    volume_range(lower=3250, col=vol_n_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.3,
                            "emp_bb": 1.5,
                            "emp_tb": 2.0,
                            "emp_sm": 0.5
                        }
                    )
                )
            ).when(
                # Hilly terrain
                # Divided
                # Two way
                # 6 lanes or more
                # Table 3-11
                expr.hilly_terrain().and_(
                    expr.divided()
                ).and_(
                    expr.lane_count(6, 'ge')
                )
            ).then(
                pl.when(
                    # Condition
                    # Volume between 0-1099
                    volume_range(0, 1099, vol_n_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.8,
                            "emp_bb": 1.6,
                            "emp_tb": 4.8,
                            "emp_sm": 0.4
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 1100-2099
                    volume_range(1100, 2099, vol_n_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.0,
                            "emp_bb": 2.0,
                            "emp_tb": 4.6,
                            "emp_sm": 0.5
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 2100-2649
                    volume_range(2100, 2649, vol_n_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.2,
                            "emp_bb": 2.3,
                            "emp_tb": 4.3,
                            "emp_sm": 0.7
                        }
                    )
                ).when(
                    # Condition
                    # Volume greater than 2650
                    volume_range(lower=2650, col=vol_n_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.8,
                            "emp_bb": 1.9,
                            "emp_tb": 3.5,
                            "emp_sm": 0.4
                        }
                    )
                )
            ).when(
                # Mountainous terrain
                # Divided
                # Two way
                # 6 lanes or more
                # Table 3-11
                expr.moutainous_terrain().and_(
                    expr.divided()
                ).and_(
                    expr.lane_count(6, 'ge')
                )
            ).then(
                pl.when(
                    # Condition
                    # Volume between 0-799
                    volume_range(0, 799, vol_n_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 3.2,
                            "emp_bb": 2.2,
                            "emp_tb": 5.5,
                            "emp_sm": 0.3
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 800-1699
                    volume_range(800, 1699, vol_n_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.9,
                            "emp_bb": 2.6,
                            "emp_tb": 5.1,
                            "emp_sm": 0.4
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 1700-2299
                    volume_range(1700, 2299, vol_n_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.6,
                            "emp_bb": 2.9,
                            "emp_tb": 4.8,
                            "emp_sm": 0.6
                        }
                    )
                ).when(
                    # Condition
                    # Volume greater than 2300
                    volume_range(lower=2300, col=vol_n_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.0,
                            "emp_bb": 2.4,
                            "emp_tb": 3.8,
                            "emp_sm": 0.3
                        }
                    )
                )
            ),
            
            # For two way divided
            PCE_O=pl.when(
                # Flat terrain
                # Divided
                # Two way
                # 5 lanes or less
                # Table 3-10
                expr.flat_terrain().and_(
                    expr.divided()
                ).and_(
                    expr.lane_count(5, 'le')
                )
            ).then(
                pl.when(
                    # Condition
                    # Volume between 0-999
                    volume_range(0, 999, vol_o_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.2,
                            "emp_bb": 1.2,
                            "emp_tb": 1.6,
                            "emp_sm": 0.5
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 1000-1799
                    volume_range(1000, 1799, vol_o_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.4,
                            "emp_bb": 1.4,
                            "emp_tb": 2.0,
                            "emp_sm": 0.6
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 1800-2149
                    volume_range(1800, 2149, vol_o_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.6,
                            "emp_bb": 1.7,
                            "emp_tb": 2.5,
                            "emp_sm": 0.8
                        }
                    )
                ).when(
                    # Condition
                    # Volume greater than 2150
                    volume_range(lower=2150, col=vol_o_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.3,
                            "emp_bb": 1.5,
                            "emp_tb": 2.0,
                            "emp_sm": 0.5
                        }
                    )
                )
            ).when(
                # Hilly terrain
                # Divided
                # Two way
                # 5 lanes or less
                # Table 3-10
                expr.hilly_terrain().and_(
                    expr.divided()
                ).and_(
                    expr.lane_count(5, 'le')
                )
            ).then(
                pl.when(
                    # Condition
                    # Volume between 0-749
                    volume_range(0, 749, vol_o_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.8,
                            "emp_bb": 1.6,
                            "emp_tb": 4.8,
                            "emp_sm": 0.4
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 750-1399
                    volume_range(750, 1399, vol_o_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.0,
                            "emp_bb": 2.0,
                            "emp_tb": 4.6,
                            "emp_sm": 0.5
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 1400-1749
                    volume_range(1400, 1749, vol_o_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.2,
                            "emp_bb": 2.3,
                            "emp_tb": 4.3,
                            "emp_sm": 0.7
                        }
                    )
                ).when(
                    # Condition
                    # Volume greater than 1750
                    volume_range(lower=1750, col=vol_o_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.8,
                            "emp_bb": 1.9,
                            "emp_tb": 3.5,
                            "emp_sm": 0.4
                        }
                    )
                )
            ).when(
                # Mountainous terrain
                # Divided
                # Two way
                # 5 lanes or less
                # Table 3-10
                expr.moutainous_terrain().and_(
                    expr.divided()
                ).and_(
                    expr.lane_count(5, 'le')
                )
            ).then(
                pl.when(
                    # Condition
                    # Volume between 0-549
                    volume_range(0, 549, vol_o_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 3.2,
                            "emp_bb": 2.2,
                            "emp_tb": 5.5,
                            "emp_sm": 0.3
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 550-1099
                    volume_range(550, 1099, vol_o_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.9,
                            "emp_bb": 2.6,
                            "emp_tb": 5.1,
                            "emp_sm": 0.4
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 1100-1499
                    volume_range(1800, 2149, vol_o_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.6,
                            "emp_bb": 2.9,
                            "emp_tb": 4.8,
                            "emp_sm": 0.6
                        }
                    )
                ).when(
                    # Condition
                    # Volume greater than 1500
                    volume_range(lower=1500, col=vol_o_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.0,
                            "emp_bb": 2.4,
                            "emp_tb": 3.8,
                            "emp_sm": 0.3
                        }
                    )
                )
            ).when(
                # Flat terrain
                # Divided
                # Two way
                # 6 lanes or more
                # Table 3-11
                expr.flat_terrain().and_(
                    expr.divided()
                ).and_(
                    expr.lane_count(6, 'ge')
                )
            ).then(
                pl.when(
                    # Condition
                    # Volume between 0-1499
                    volume_range(0, 1499, vol_o_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.2,
                            "emp_bb": 1.2,
                            "emp_tb": 1.6,
                            "emp_sm": 0.5
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 1500-2749
                    volume_range(1500, 2749, vol_o_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.4,
                            "emp_bb": 1.4,
                            "emp_tb": 2.0,
                            "emp_sm": 0.6
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 2750-3249
                    volume_range(2750, 3249, vol_o_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.6,
                            "emp_bb": 1.7,
                            "emp_tb": 2.5,
                            "emp_sm": 0.8
                        }
                    )
                ).when(
                    # Condition
                    # Volume greater than 3250
                    volume_range(lower=3250, col=vol_o_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.3,
                            "emp_bb": 1.5,
                            "emp_tb": 2.0,
                            "emp_sm": 0.5
                        }
                    )
                )
            ).when(
                # Hilly terrain
                # Divided
                # Two way
                # 6 lanes or more
                # Table 3-11
                expr.hilly_terrain().and_(
                    expr.divided()
                ).and_(
                    expr.lane_count(6, 'ge')
                )
            ).then(
                pl.when(
                    # Condition
                    # Volume between 0-1099
                    volume_range(0, 1099, vol_o_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.8,
                            "emp_bb": 1.6,
                            "emp_tb": 4.8,
                            "emp_sm": 0.4
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 1100-2099
                    volume_range(1100, 2099, vol_o_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.0,
                            "emp_bb": 2.0,
                            "emp_tb": 4.6,
                            "emp_sm": 0.5
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 2100-2649
                    volume_range(2100, 2649, vol_o_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.2,
                            "emp_bb": 2.3,
                            "emp_tb": 4.3,
                            "emp_sm": 0.7
                        }
                    )
                ).when(
                    # Condition
                    # Volume greater than 2650
                    volume_range(lower=2650, col=vol_o_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 1.8,
                            "emp_bb": 1.9,
                            "emp_tb": 3.5,
                            "emp_sm": 0.4
                        }
                    )
                )
            ).when(
                # Mountainous terrain
                # Divided
                # Two way
                # 6 lanes or more
                # Table 3-11
                expr.moutainous_terrain().and_(
                    expr.divided()
                ).and_(
                    expr.lane_count(6, 'ge')
                )
            ).then(
                pl.when(
                    # Condition
                    # Volume between 0-799
                    volume_range(0, 799, vol_o_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 3.2,
                            "emp_bb": 2.2,
                            "emp_tb": 5.5,
                            "emp_sm": 0.3
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 800-1699
                    volume_range(800, 1699, vol_o_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.9,
                            "emp_bb": 2.6,
                            "emp_tb": 5.1,
                            "emp_sm": 0.4
                        }
                    )
                ).when(
                    # Condition
                    # Volume between 1700-2299
                    volume_range(1700, 2299, vol_o_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.6,
                            "emp_bb": 2.9,
                            "emp_tb": 4.8,
                            "emp_sm": 0.6
                        }
                    )
                ).when(
                    # Condition
                    # Volume greater than 2300
                    volume_range(lower=2300, col=vol_o_col)
                ).then(
                    pl.lit(
                        {
                            "emp_ks": 2.0,
                            "emp_bb": 2.4,
                            "emp_tb": 3.8,
                            "emp_sm": 0.3
                        }
                    )
                )
            )
        )

        # Road stat = K
        # Kota
        k_lf = joined.filter(
            expr.k_stat()
        ).with_columns(
            # For undivided two way and one way
            PCE=pl.when(
                expr.undivided().and_(
                    expr.two_way()
                )
            ).then(
                pl.when(
                    # Total volume less than 1800
                    # Lane width less than or equal to 6
                    # Table 4-10
                    pl.col(total_vol_col).lt(1800).and_(
                        pl.col(ctx.total_lanew_col).le(6)
                    )
                ).then(
                    pl.lit(
                        {
                            'emp_ks': 1.3,
                            'emp_bb': 1.3,
                            'emp_tb': 1.3,
                            'emp_sm': 0.5
                        }
                    )
                ).when(
                    # Total volume less than 1800
                    # Lane width less than or equal to 6
                    # Table 4-10
                    pl.col(total_vol_col).lt(1800).and_(
                        pl.col(ctx.total_lanew_col).gt(6)
                    )
                ).then(
                    pl.lit(
                        {
                            'emp_ks': 1.3,
                            'emp_bb': 1.3,
                            'emp_tb': 1.3,
                            'emp_sm': 0.4
                        }
                    )
                ).when(
                    # Total volume greater than or equal to 1800
                    # Lane width less than or equal to 6
                    # Table 4-10
                    pl.col(total_vol_col).ge(1800).and_(
                        pl.col(ctx.total_lanew_col).le(6)
                    )
                ).then(
                    pl.lit(
                        {
                            'emp_ks': 1.2,
                            'emp_bb': 1.2,
                            'emp_tb': 1.2,
                            'emp_sm': 0.35
                        }
                    )
                ).when(
                    # Total volume greater than or equal to 1800
                    # Lane width greater than 6
                    # Table 4-10
                    pl.col(total_vol_col).ge(1800).and_(
                        pl.col(ctx.total_lanew_col).gt(6)
                    )
                ).then(
                    pl.lit(
                        {
                            'emp_ks': 1.2,
                            'emp_bb': 1.2,
                            'emp_tb': 1.2,
                            'emp_sm': 0.25
                        }
                    )
                )
            ).when(
                # One way with 2 or less lane
                expr.one_way().and_(
                    expr.lane_count(2, 'le')
                )
            ).then(
                pl.when(
                    # Volume less 1050
                    # Table 4-11
                    pl.col(total_vol_col).lt(1050)
                ).then(
                    pl.lit(
                        {
                            'emp_ks': 1.3,
                            'emp_bb': 1.3,
                            'emp_tb': 1.3,
                            'emp_sm': 0.4
                        }
                    )
                ).when(
                    # Volume greater than or equal to 1050
                    # Table 4-11
                    pl.col(total_vol_col).ge(1050)
                ).then(
                    pl.lit(
                        {
                            'emp_ks': 1.2,
                            'emp_bb': 1.2,
                            'emp_tb': 1.2,
                            'emp_sm': 0.25
                        }
                    )
                )
            ).when(
                # One way with 3 lanes or more
                expr.one_way().and_(
                    expr.lane_count(3, 'ge')
                )
            ).then(
                # Volume less than 1100
                pl.when(
                    pl.col(total_vol_col).lt(1100)
                ).then(
                    pl.lit(
                        {
                            'emp_ks': 1.3,
                            'emp_bb': 1.3,
                            'emp_tb': 1.3,
                            'emp_sm': 0.4
                        }
                    )
                ).when(
                    pl.col(total_vol_col).ge(1100)
                ).then(
                    pl.lit(
                        {
                            'emp_ks': 1.2,
                            'emp_bb': 1.2,
                            'emp_tb': 1.2,
                            'emp_sm': 0.25
                        }
                    )
                )
            ),

            # For two way and divided
            PCE_N=pl.when(
                # Divided
                # Two way with 5 lanes or less
                expr.two_way().and_(
                    expr.lane_count(5, 'le')
                ).and_(
                    expr.divided()
                )
            ).then(
                pl.when(
                    # Volume less 1050
                    # Table 4-11
                    pl.col(vol_n_col).lt(1050)
                ).then(
                    pl.lit(
                        {
                            'emp_ks': 1.3,
                            'emp_bb': 1.3,
                            'emp_tb': 1.3,
                            'emp_sm': 0.4
                        }
                    )
                ).when(
                    # Volume greater than or equal to 1050
                    # Table 4-11
                    pl.col(vol_n_col).ge(1050)
                ).then(
                    pl.lit(
                        {
                            'emp_ks': 1.2,
                            'emp_bb': 1.2,
                            'emp_tb': 1.2,
                            'emp_sm': 0.25
                        }
                    )
                )
            ).when(
                # Two way with 6 lanes or more
                expr.two_way().and_(
                    expr.lane_count(6, 'ge')
                ).and_(
                    expr.divided()
                )
            ).then(
                # Volume less than 1100
                pl.when(
                    pl.col(vol_n_col).lt(1100)
                ).then(
                    pl.lit(
                        {
                            'emp_ks': 1.3,
                            'emp_bb': 1.3,
                            'emp_tb': 1.3,
                            'emp_sm': 0.4
                        }
                    )
                ).when(
                    pl.col(vol_n_col).ge(1100)
                ).then(
                    pl.lit(
                        {
                            'emp_ks': 1.2,
                            'emp_bb': 1.2,
                            'emp_tb': 1.2,
                            'emp_sm': 0.25
                        }
                    )
                )
            ),
            
            # For two way and divided
            PCE_O=pl.when(
                # Divided
                # Two way with 5 lanes or less
                expr.two_way().and_(
                    expr.lane_count(5, 'le')
                ).and_(
                    expr.divided()
                )
            ).then(
                pl.when(
                    # Volume less 1050
                    # Table 4-11
                    pl.col(vol_o_col).lt(1050)
                ).then(
                    pl.lit(
                        {
                            'emp_ks': 1.3,
                            'emp_bb': 1.3,
                            'emp_tb': 1.3,
                            'emp_sm': 0.4
                        }
                    )
                ).when(
                    # Volume greater than or equal to 1050
                    # Table 4-11
                    pl.col(vol_o_col).ge(1050)
                ).then(
                    pl.lit(
                        {
                            'emp_ks': 1.2,
                            'emp_bb': 1.2,
                            'emp_tb': 1.2,
                            'emp_sm': 0.25
                        }
                    )
                )
            ).when(
                # Two way with 6 lanes or more
                expr.two_way().and_(
                    expr.lane_count(6, 'ge')
                ).and_(
                    expr.divided()
                )
            ).then(
                # Volume less than 1100
                pl.when(
                    pl.col(vol_o_col).lt(1100)
                ).then(
                    pl.lit(
                        {
                            'emp_ks': 1.3,
                            'emp_bb': 1.3,
                            'emp_tb': 1.3,
                            'emp_sm': 0.4
                        }
                    )
                ).when(
                    pl.col(vol_o_col).ge(1100)
                ).then(
                    pl.lit(
                        {
                            'emp_ks': 1.2,
                            'emp_bb': 1.2,
                            'emp_tb': 1.2,
                            'emp_sm': 0.25
                        }
                    )
                )
            )
        )   

        lf = pl.concat(
            [lk_lf, k_lf]
        ).select(
            [
                ctx.linkid_col,
                ctx.from_sta_col,
                ctx.to_sta_col,
                ctx.survey_date_col,
                ctx.survey_hours_col,
                'PCE',
                'PCE_N',
                'PCE_O'
            ] + [
                f"{_col}_{_dir}" for _dir in ['O', 'N'] for _col in [
                    ctx.veh1_col, ctx.veh2_col, ctx.veh3_col, ctx.veh4_col, ctx.veh5_col
                ]
            ]
        )

        out_ctx = PipelineContext()
        out_ctx.lf = lf

        return out_ctx


class VolumePCECalculation(PipelineStep):
    def __init__(self):
        super().__init__('PCE_volume_calculation')
    
    def execute(self, ctx: PipelineContext) -> PipelineContext:
        if 'PCE_N' not in ctx.lf.collect_schema().names():
            raise ValueError("LazyFrame does not have 'PCE_N' columns")

        if 'PCE' not in ctx.lf.collect_schema().names():
            raise ValueError("LazyFrame does not have 'PCE' columns")

        if 'PCE_O' not in ctx.lf.collect_schema().names():
            raise ValueError("LazyFrame does not have 'PCE_O' columns")
                
        def pce_expression(side: Literal['N', 'O']) -> pl.LazyFrame:
            lf = ctx.lf.filter(
                pl.col(f'PCE_{side}').is_not_null()
            ).select(
                ctx.linkid_col,
                ctx.from_sta_col,
                ctx.to_sta_col,
                ctx.survey_date_col,
                ctx.survey_hours_col,
                PCE_VEH1=pl.col(f"{ctx.veh1_col}_{side}").mul(
                    pl.col(f'PCE_{side}').struct.field('emp_sm')
                ),
                PCE_VEH2=pl.col(f'{ctx.veh2_col}_{side}'),
                PCE_VEH3=pl.col(f"{ctx.veh3_col}_{side}").mul(
                    pl.col(f'PCE_{side}').struct.field('emp_ks')
                ),
                PCE_VEH4=pl.col(f"{ctx.veh4_col}_{side}").mul(
                    pl.col(f'PCE_{side}').struct.field('emp_bb')
                ),
                PCE_VEH5=pl.col(f"{ctx.veh5_col}_{side}").mul(
                    pl.col(f'PCE_{side}').struct.field('emp_tb')
                ),
                DIR=pl.lit(side)
            )

            return lf

        pce_n = pce_expression('N')
        pce_o = pce_expression('O')
        pce_on = ctx.lf.filter(
            pl.col('PCE').is_not_null()
        ).select(
            ctx.linkid_col,
            ctx.from_sta_col,
            ctx.to_sta_col,
            ctx.survey_date_col,
            ctx.survey_hours_col,
            PCE_VEH1=pl.col(f"{ctx.veh1_col}_N").add(
                pl.col(f"{ctx.veh1_col}_O")
            ).mul(
                pl.col('PCE').struct.field('emp_sm')
            ),
            PCE_VEH2=pl.col(f'{ctx.veh2_col}_N').add(
                pl.col(f'{ctx.veh2_col}_O')
            ),
            PCE_VEH3=pl.col(f"{ctx.veh3_col}_N").add(
                pl.col(f"{ctx.veh3_col}_O")
            ).mul(
                pl.col(f'PCE').struct.field('emp_ks')
            ),
            PCE_VEH4=pl.col(f"{ctx.veh4_col}_N").add(
                pl.col(f"{ctx.veh4_col}_O")
            ).mul(
                pl.col(f'PCE').struct.field('emp_bb')
            ),
            PCE_VEH5=pl.col(f"{ctx.veh5_col}_N").add(
                pl.col(f"{ctx.veh5_col}_O")
            ).mul(
                pl.col(f'PCE').struct.field('emp_tb')
            ),
            DIR=pl.lit('ON')
        )

        lf = pl.concat(
            [pce_n, pce_o, pce_on]
        ).with_columns(
            TOTAL_PCE=pl.sum_horizontal(
                ['PCE_VEH1', 'PCE_VEH2', 'PCE_VEH3', 'PCE_VEH4', 'PCE_VEH5']
            )
        )

        out_ctx = PipelineContext()
        out_ctx.lf = lf

        return out_ctx