import polars as pl
from .pipeline import PipelineContext
from typing import Literal


class CapacityExpressions:
    """
    Polars expression for VCR calculation.
    """
    def __init__(self, ctx: PipelineContext):
        self.ctx = ctx

    def k_stat(self) -> pl.Expr:
        """
        'K' road status condition
        """
        return pl.col(self.ctx.road_stat_col).eq('K')
    
    def lk_stat(self) -> pl.Expr:
        """
        'LK' road status condition
        """
        return pl.col(self.ctx.road_stat_col).eq('LK')
    
    def one_way(self) -> pl.Expr:
        """
        One way query, has only single direction.
        """
        return pl.col(self.ctx.dir_count_col).eq(1)
    
    def two_way(self) -> pl.Expr:
        """
        Two way query, has two directions.
        """
        return pl.col(self.ctx.dir_count_col).eq(2)
    
    def divided(self) -> pl.Expr:
        """
        Has median.
        """
        return pl.col(self.ctx.has_med_col).eq(1)
    
    def undivided(self) -> pl.Expr:
        """
        Does not has median.
        """
        return pl.col(self.ctx.has_med_col).eq(0)
    
    def lane_count(self, lanes: int, op: Literal['le', 'ge', 'gt', 'lt', 'eq']) -> pl.Expr:
        """
        Has specific number of lanes, with operator le, ge, gt or lt.
        """
        return getattr(pl.col(self.ctx.lane_count_col), op)(lanes)
    
    def flat_terrain(self) -> pl.Expr:
        """
        Has flat terrain.
        """
        return pl.col(self.ctx.terrain_col).eq(1)
    
    def hilly_terrain(self) -> pl.Expr:
        """
        Has hilly terrain.
        """
        return pl.col(self.ctx.terrain_col).eq(2)
    
    def moutainous_terrain(self) -> pl.Expr:
        """
        Has mountainous terrain.
        """
        return pl.col(self.ctx.terrain_col).eq(3)