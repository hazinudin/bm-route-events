from .pipeline import PipelineStep, PipelineContext
import polars as pl
from .expr import CapacityExpressions
from copy import deepcopy


class CapacityC0(PipelineStep):
    def __init__(self):
        super().__init__(step_name='c0_lookup')
    
    def execute(self, ctx: PipelineContext) -> PipelineContext:
        """
        Join RNI data with C0 (base capacity).
        """
        expr = CapacityExpressions(ctx)

        # Create columns for C0 in LazyFrame
        lf = ctx.lf.with_columns(
            C0=pl.when(
                # Condition:
                # Road status = K 
                # Single direction OR two way and divided
                expr.k_stat().and_(
                    expr.one_way().or_(
                        expr.two_way().and_(
                            expr.divided()
                        )
                    )
                )
            ).then(
                pl.col(ctx.lane_count_col).mul(1700)
            ).when(
                # Condition:
                # Road status = K
                # Without median
                # Two direction
                # Total lane count <= 3
                expr.k_stat().and_(
                    expr.undivided()
                ).and_(
                    expr.two_way()
                ).and_(
                    expr.lane_count(3, 'le')
                )
            ).then(
                pl.lit(2800)
            ).when(
                # Condition:
                # Road status = K
                # Without median
                # Two direction
                # Total lane count >= 4
                expr.k_stat().and_(
                    expr.undivided()
                ).and_(
                    expr.two_way()
                ).and_(
                    expr.lane_count(4, 'ge')
                )
            ).then(
                pl.col(ctx.lane_count_col).mul(1500)
            ).when(
                # Condition:
                # Road status = LK
                # 4 or more lanes, 2 dir, no median
                # Terrain = 1 (Flat/Datar)
                expr.lk_stat().and_(
                    expr.lane_count(4, 'ge').and_(
                        expr.two_way()
                    ).and_(
                        expr.undivided()
                    )
                ).and_(
                    expr.flat_terrain()
                )
            ).then(
                pl.col(ctx.lane_count_col).mul(1900)
            ).when(
                # Condition:
                # Road status = LK
                # 4 or more lanes, 2 dir, no median
                # Terrain = 2 (Hilly/Bukit)
                expr.lk_stat().and_(
                    expr.lane_count(4, 'ge').and_(
                        expr.two_way()
                    ).and_(
                        expr.undivided()
                    )
                ).and_(
                    expr.hilly_terrain()
                )
            ).then(
                pl.col(ctx.lane_count_col).mul(1850)
            ).when(
                # Condition:
                # Road status = LK
                # 4 or more lanes, 2 dir, no median
                # Terrain = 3 (Mountain/Gunung)
                expr.lk_stat().and_(
                    expr.lane_count(4, 'ge').and_(
                        expr.two_way()
                    ).and_(
                        expr.undivided()
                    )
                ).and_(
                    expr.moutainous_terrain()
                )
            ).then(
                pl.col(ctx.lane_count_col).mul(1800)
            ).when(
                # Condition:
                # Road status = LK
                # Without median
                # Two direction
                # Total lane count <= 3
                # Terrain = 1 (Flat/Datar)
                expr.lk_stat().and_(
                    expr.undivided()
                ).and_(
                    expr.two_way()
                ).and_(
                    expr.lane_count(3, 'le')
                ).and_(
                    expr.flat_terrain()
                )
            ).then(
                pl.lit(4000)
            ).when(
                # Condition:
                # Road status = LK
                # Without median
                # Two direction
                # Total lane count <= 3
                # Terrain = 2 (Hilly/Bukit)
                expr.lk_stat().and_(
                    expr.undivided()
                ).and_(
                    expr.two_way()
                ).and_(
                    expr.lane_count(3, 'le')
                ).and_(
                    expr.hilly_terrain()
                )
            ).then(
                pl.lit(3850)
            ).when(
                # Condition:
                # Road status = LK
                # Without median
                # Two direction
                # Total lane count <= 3
                # Terrain = 3 (Mountain/Gunung)
                expr.lk_stat().and_(
                    expr.undivided()
                ).and_(
                    expr.two_way()
                ).and_(
                    expr.lane_count(3, 'le')
                ).and_(
                    expr.moutainous_terrain()
                )
            ).then(
                pl.lit(3700)
            ).when(
                # Condition:
                # Road status = LK
                # With median
                # Two direction
                # OR one direction
                # Terrain = 1 (Flat/Datar)
                expr.lk_stat().and_(
                    expr.one_way().or_(
                        expr.divided().and_(
                            expr.two_way()
                        )
                    )
                ).and_(
                    expr.flat_terrain()
                )
            ).then(
                pl.col(ctx.lane_count_col).mul(2200)
            ).when(
                # Condition:
                # Road status = LK
                # With median
                # Two direction
                # OR one direction
                # Terrain = 2 (Hilly/Bukit)
                expr.lk_stat().and_(
                    expr.one_way().or_(
                        expr.divided().and_(
                            expr.two_way()
                        )
                    )
                ).and_(
                    expr.hilly_terrain()
                )
            ).then(
                pl.col(ctx.lane_count_col).mul(2100)
            ).when(
                # Condition:
                # Road status = LK
                # With median
                # Two direction
                # OR one direction
                # Terrain = 3 (Mountain/Gunung)
                expr.lk_stat().and_(
                    expr.one_way().or_(
                        expr.divided().and_(
                            expr.two_way()
                        )
                    )
                ).and_(
                    expr.moutainous_terrain()
                )
            ).then(
                pl.col(ctx.lane_count_col).mul(2000)
            )
        )

        # One way
        one_way_or_undivided = lf.filter(
            expr.one_way().or_(
                expr.undivided()
            )
        ).select(
            [
                ctx.linkid_col,
                ctx.from_sta_col,
                ctx.to_sta_col,
                pl.col('C0').cast(pl.Float64)
            ],
            **{ctx.dir_col:pl.lit('ON')}
        )

        # C0 for left lanes 
        divided_left = lf.filter(
            expr.divided().and_(expr.two_way())
        ).select(
            ctx.linkid_col,
            ctx.from_sta_col,
            ctx.to_sta_col,
            C0=pl.col('C0').truediv(
                pl.col(ctx.lane_count_col)
            ).mul(
                pl.col(ctx.llane_count_col)
            ),
            **{ctx.dir_col:pl.lit('N')}
        )

        # C0 for right lanes
        divided_right = lf.filter(
            expr.divided().and_(expr.two_way())
        ).select(
            ctx.linkid_col,
            ctx.from_sta_col,
            ctx.to_sta_col,
            C0=pl.col('C0').truediv(
                pl.col(ctx.lane_count_col)
            ).mul(
                pl.col(ctx.rlane_count_col)
            ),
            **{ctx.dir_col:pl.lit('O')}
        )

        # Update the context LazyFrame
        ctx_out = deepcopy(ctx)
        ctx_out.lf = pl.concat(
            [one_way_or_undivided, divided_left, divided_right]
        )

        return ctx_out