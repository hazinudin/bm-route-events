import unittest
from route_events.geometry import Points, Point
from route_events.geometry import LAMBERT_WKT
from polars import DataFrame, col


class TestPoints(unittest.TestCase):
    def test_transform(self):
        """
        Transform Point coordinate from EPSG:4326 to Lambert projection.
        """
        df = DataFrame([
            {"long": 95.42103999972832, "lat": 5.647860000331377},
            {"long": 95.42103999972832, "lat": 5.647860000331377}
            ])
        
        pt = Points(df, long_col="long", lat_col="lat", wkt='EPSG:4326')
        transformed = pt.transform(LAMBERT_WKT, invert=True)

        self.assertTrue(transformed._rows.filter(
            (col('long') == -2184157.971000001) & (col('lat') == 609253.9258999936)
            ).shape == (2,2))

    def test_distance_to_point(self):
        """
        Calculate distance between two Points.
        """
        df = DataFrame([
                {"long": 95.42103999972832, "lat": 5.647860000331377},
                {"long": 95.42103999972832, "lat": 5.647860000331377}
                ])
        
        pt = Points(df, long_col="long", lat_col="lat", wkt='EPSG:4326')
        pt = pt.transform(LAMBERT_WKT, invert=True)

        other_pt = Point(long=pt._rows['lat'][0]+30, lat=pt._rows['long'][0], wkt=LAMBERT_WKT)  # Shift 30 meters right

        dist = pt.distance_to_point(other_pt)

        self.assertTrue(dist.fetchone()[0] == 30)

    def test_h3_index(self):
        """
        Calculate h3 index for every points in the table.
        """
        df = DataFrame([
                {"long": 95.42103999972832, "lat": 5.647860000331377},
                {"long": 95.42103999972832, "lat": 5.647860000331377}
                ])
        
        pt = Points(df, long_col='long', lat_col='lat', wkt='EPSG:4326')
        h3_df = pt.h3_indexed(resolution=13)

        self.assertTrue(h3_df['h3'][0] == '8d6552243ac657f')

    def test_points_with_id(self):
        """
        Test input point with ID
        """
        df = DataFrame([
            {"long": 95.42103999972832, "lat": 5.647860000331377, "id": 'a', "id1": 1},
            {"long": 95.42103999972832, "lat": 5.647860000331377, "id": 'b', "id1": 2}
            ])
        
        pt = Points(df, long_col="long", lat_col="lat", wkt='EPSG:4326',
                    ids_column=["id", "id1"])
        transformed = pt.transform(LAMBERT_WKT, invert=True)

        self.assertTrue(transformed._rows.filter(
            (col('long') == -2184157.971000001) & (col('lat') == 609253.9258999936)
            ).shape == (2,4))
        
    def test_nearest(self):
        """
        Test nearest method.
        """
        df = DataFrame([
            {"long": 95.42103999972832, "lat": 5.647860000331377, "id": 'a', "id1": 1},
            {"long": 95.42103999972832, "lat": 5.647860000331377, "id": 'b', "id1": 2}
        ])

        pt = Points(
            df,
            long_col="long",
            lat_col="lat",
            wkt="EPSG:4326",
            ids_column=["id", "id1"]
        )
        transformed = pt.transform(LAMBERT_WKT, invert=True)

        pt.nearest(pt.first())
        
        self.assertTrue(True)
