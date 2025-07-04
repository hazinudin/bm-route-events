from .pipeline import PipelineContext, PipelineStep, MultiDataContext
import polars as pl
from .expr import CapacityExpressions
from copy import deepcopy


class CapacityFCHSLookup(PipelineStep):
    """
    Capacity FCHS lookup step.
    """
    def __init__(self):
        super().__init__(step_name='fchs_value')
    
    def execute(self, ctx: MultiDataContext) -> PipelineContext:
        if (
            'RNI' not in ctx.datas.keys()
        ) or (
            'VOLH' not in ctx.datas.keys()
        ):
            raise KeyError('Context does not contain RNI or VOLH dataset')
        
        expr = CapacityExpressions(ctx)
        shwidth_range = [0.5, 1.0, 1.5, 2.0]
        shwidth_struct_field = 'shwidth'
        fchs_struct_field = 'fchs'

        # Road status K Non-Motorized frequency category object.
        class _K_NonMotorClass:
            def __init__(self, ctx: MultiDataContext):
                self.very_low = pl.col(ctx.non_motor_col).lt(100)

                self.low = pl.col(ctx.non_motor_col).ge(100).and_(
                    pl.col(ctx.non_motor_col).le(299)
                )

                self.medium = pl.col(ctx.non_motor_col).ge(300).and_(
                    pl.col(ctx.non_motor_col).le(499)
                )

                self.high = pl.col(ctx.non_motor_col).ge(500).and_(
                    pl.col(ctx.non_motor_col).le(899)
                )

                self.very_high = pl.col(ctx.non_motor_col).ge(900)


        # Road status K shoulder width category object.
        class _K_ShoulderWidthClass:
            def __init__(self, ctx: MultiDataContext):
                self.le_0_5 = pl.col(ctx.shwidth_col).le(0.5)

                self.from_0_5_to_1 = pl.col(ctx.shwidth_col).gt(0.5).and_(
                    pl.col(ctx.shwidth_col).le(1.0)
                )

                self.from_1_to_1_5 = pl.col(ctx.shwidth_col).gt(1.0).and_(
                    pl.col(ctx.shwidth_col).le(1.5)
                )

                self.from_1_5_to_2 = pl.col(ctx.shwidth_col).gt(1.5).and_(
                    pl.col(ctx.shwidth_col).lt(2.0)
                )

                self.ge_2 = pl.col(ctx.shwidth_col).ge(2.0)

                
        k_nm = _K_NonMotorClass(ctx)
        k_sh = _K_ShoulderWidthClass(ctx)
    
        # Aggregate the VOLH to get the average Non-Motorized hourly frequency
        nm_hourly = ctx.datas['VOLH'].group_by(
            ctx.linkid_col,
            ctx.survey_date_col,
            ctx.survey_hours_col
        ).agg(
            pl.col(ctx.non_motor_col).sum()
        ).group_by(
            ctx.linkid_col
        ).agg(
            pl.col(ctx.non_motor_col).max()
        )

        # Join the RNI data with Non-Motorized data
        lf = ctx.datas['RNI'].join(
            nm_hourly,
            left_on=ctx.join_key['RNI'],
            right_on=ctx.join_key['VOLH']
        ).with_columns(
            # Adjustment factor for non-motorized count in K stat route
            # Table 4-8
            pl.col(ctx.non_motor_col).mul(0.4)
        ).with_columns(
            FCHS_STRUCT=pl.when(
                # Condition
                # Road status = K
                # Divided
                expr.k_stat().and_(
                    expr.divided()
                )
            ).then(
                # Table 4-5 page 108
                pl.when(
                    k_nm.very_low
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.96, 0.98, 1.01, 1.03],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                ).when(
                    k_nm.low
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.94, 0.97, 1.0, 1.02],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                ).when(
                    k_nm.medium
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.92, 0.95, 0.98, 1.0],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                ).when(
                    k_nm.high
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.88, 0.92, 0.95, 0.98],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                ).when(
                    k_nm.very_high
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.84, 0.88, 0.92, 0.96],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                )
            ).when(
                # Condition
                # Road status = K
                # Undivided two way (lane count<=3) OR one way
                expr.k_stat().and_(
                    expr.one_way().or_(
                        expr.two_way().and_(
                            expr.undivided()
                        ).and_(
                            expr.lane_count(3, 'le')
                        )
                    )
                )
            ).then(
                # Table 4-5 page 108
                pl.when(
                    k_nm.very_low
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.94, 0.96, 0.99, 1.01],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                ).when(
                    k_nm.low
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.92, 0.94, 0.97, 1.0],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                ).when(
                    k_nm.medium
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.89, 0.92, 0.95, 0.98],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                ).when(
                    k_nm.high
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.82, 0.86, 0.90, 0.95],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                ).when(
                    k_nm.very_high
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.73, 0.79, 0.85, 0.91],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                )
            ).when(
                # Condition
                # Road status = K
                # Undivided two way (lane count>=4)
                expr.k_stat().and_(
                    expr.two_way().and_(
                        expr.undivided()
                    ).and_(
                        expr.lane_count(4, 'ge')
                    )
                )
            ).then(
                # Table 11-11 page 277
                pl.when(
                    k_nm.very_low
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.96, 0.99, 1.01, 1.03],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                ).when(
                    k_nm.low
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.94, 0.97, 1.0, 1.02],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                ).when(
                    k_nm.medium
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.92, 0.95, 0.98, 1.0],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                ).when(
                    k_nm.high
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.87, 0.91, 0.94, 0.98],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                ).when(
                    k_nm.very_high
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.80, 0.86, 0.90, 0.96],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                )
            ).when(
                # Condition
                # Road status = LK
                # Divided
                # Two direction
                # OR single direction
                expr.lk_stat().and_(
                    expr.one_way().or_(
                        expr.divided().and_(
                            expr.two_way()
                        )    
                    )
                )
            ).then(
                # Table 3-8 page 70
                pl.when(
                    k_nm.very_low
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.99, 1.0, 1.01, 1.03],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                ).when(
                    k_nm.low
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.96, 0.97, 0.99, 1.01],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                ).when(
                    k_nm.medium
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.93, 0.95, 0.96, 0.99],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                ).when(
                    k_nm.high
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.9, 0.92, 0.95, 0.97],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                ).when(
                    k_nm.very_high
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.88, 0.9, 0.93, 0.96],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                )
            ).when(
                # Condition
                # Road status = LK
                # Undivided
                # Two direction
                # Total lane count <= 3
                expr.lk_stat().and_(
                    expr.undivided()
                ).and_(
                    expr.two_way()
                ).and_(
                    expr.lane_count(3, 'le')
                )
            ).then(
                # Table 3-8 page 70
                pl.when(
                    k_nm.very_low
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.97, 0.99, 1.0, 1.02],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                ).when(
                    k_nm.low
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.93, 0.95, 0.97, 1.0],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                ).when(
                    k_nm.medium
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.88, 0.91, 0.94, 0.98],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                ).when(
                    k_nm.high
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.84, 0.87, 0.91, 0.95],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                ).when(
                    k_nm.very_high
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.8, 0.83, 0.88, 0.93],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                )
            ).when(
                # Condition
                # Road status = LK
                # Undivided
                # Two direction
                # Total lane count >= 4
                expr.lk_stat().and_(
                    expr.one_way().or_(
                        expr.undivided().and_(
                            expr.two_way()
                        ).and_(
                            expr.lane_count(4, 'ge')
                        )
                    )
                )
            ).then(
                # Table 10-7 page 217
                pl.when(
                    k_nm.very_low
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [1.0, 1.0, 1.0, 1.0],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                ).when(
                    k_nm.low
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.96, 0.97, 0.97, 0.98],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                ).when(
                    k_nm.medium
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.92, 0.94, 0.95, 0.97],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                ).when(
                    k_nm.high
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.88, 0.89, 0.9, 0.96],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                ).when(
                    k_nm.very_high
                ).then(
                    pl.lit(
                        {
                            fchs_struct_field: [0.81, 0.83, 0.85, 0.95],
                            shwidth_struct_field: shwidth_range
                        }
                    )
                )
            )
        )

        out_ctx = PipelineContext()
        out_ctx.lf = lf

        return out_ctx
    

class CapacityFCHSInterpolation(PipelineStep):
    def __init__(self):
        super().__init__(step_name='fchs_interpolation')

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        """
        Interpolate FCHS value based on shoulder width.
        """
        if 'FCHS_STRUCT' not in ctx.lf.collect_schema().names():
            raise ValueError("'FCHS_STRUCT' not in the context LazyFrame columns.")
        
        expr = CapacityExpressions(ctx)
        
        def fchs_shoulder_check(sh_col: str) -> pl.Expr:
            """
            Check the shoulder width value if it is exceed the lookup value range.
            """
            return pl.when(
                pl.col(sh_col).ge(pl.col('shwidth').list.max())
            ).then(
                pl.col('fchs').list.concat(pl.col('fchs').list.max())
            ).when(
                pl.col(sh_col).le(pl.col('shwidth').list.min())
            ).then(
                pl.col('fchs').list.concat(pl.col('fchs').list.min())
            ).otherwise(
                pl.col('fchs').list.concat(None)  # Append null value that will be interpolated
            )
        
        lf = ctx.lf.unnest(
            'FCHS_STRUCT'
        ).with_columns(
            # Concat/append the FCHS value if the value exceeds the range, otherwise concat null
            fchs_n=pl.when(
                # Condition
                # divided
                expr.divided()
            ).then(
                fchs_shoulder_check(ctx.l_out_shwidth_col)
            ),
            fchs_o=pl.when(
                # Condition
                # divided
                expr.divided()
            ).then(
                fchs_shoulder_check(ctx.r_out_shwidth_col)
            ),
            fchs_on=pl.when(
                # Condition
                # Undivided OR one way
                expr.undivided().or_(expr.one_way())
            ).then(
                fchs_shoulder_check(ctx.shwidth_col)
            ),

            # Concat/append the shoulder width for interpolation variable.
            shwidth_n=pl.when(
                # Condition
                # divided
                expr.divided()
            ).then(
                pl.col('shwidth').list.concat(pl.col(ctx.l_out_shwidth_col))
            ),
            shwidth_o=pl.when(
                # Condition
                # divided
                expr.divided()
            ).then(
                pl.col('shwidth').list.concat(pl.col(ctx.r_out_shwidth_col))
            ),
            shwidth_on=pl.when(
                # Condition
                # Undivided OR one way
                expr.undivided().or_(expr.one_way())
            ).then(
                pl.col('shwidth').list.concat(pl.col(ctx.shwidth_col))
            )
        )
        
        lfs = []
        for fchs_sh_col in [['fchs_on', 'shwidth_on', 'on'], ['fchs_n', 'shwidth_n', 'n'], ['fchs_o', 'shwidth_o', 'o']]:
            fchs_col = fchs_sh_col[0]
            shwidth_col = fchs_sh_col[1]
            dir_side = fchs_sh_col[2].upper()
            
            lf_ = lf.filter(
                pl.col(fchs_col).is_not_null()
            ).explode(
                fchs_col, shwidth_col
            ).with_columns(
                # Default value
                pl.col(shwidth_col).fill_null(1)
            ).group_by(
                [
                    ctx.linkid_col,
                    ctx.from_sta_col,
                    ctx.to_sta_col
                ]
            ).agg(
                FCHS=pl.col(fchs_col).interpolate_by(shwidth_col).last()
            ).select(
                [
                    ctx.linkid_col,
                    ctx.from_sta_col,
                    ctx.to_sta_col,
                    'FCHS',
                ],
                **{ctx.dir_col: pl.lit(dir_side)}
            )

            lfs.append(lf_)

        out_ctx = deepcopy(ctx)
        out_ctx.lf = pl.concat(lfs, how='diagonal')

        return out_ctx
