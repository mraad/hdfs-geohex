import math


class Mercator:
    def __init__(self):
        pass

    @staticmethod
    def to_wgs84(x, y):
        rad = 6378137.0
        lat = (1.5707963267948966 - (2.0 * math.atan(math.exp((-1.0 * y) / rad)))) * (180 / math.pi)
        lon = ((x / rad) * 57.295779513082323) - (
            (math.floor((((x / rad) * 57.295779513082323) + 180.0) / 360.0)) * 360.0)
        return lon, lat

    @staticmethod
    def to_web_mercator(lon, lat):
        rad = 6378137.0
        e = lon * 0.017453292519943295
        x = rad * e
        n = lat * 0.017453292519943295
        sin_n = math.sin(n)
        y = 3189068.5 * math.log((1.0 + sin_n) / (1.0 - sin_n))
        return x, y
