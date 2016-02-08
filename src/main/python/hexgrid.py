import math


class HexGrid:
    def __init__(self, size=100):
        self.two_pi = 2.0 * math.pi
        self.rad_to_deg = 180.0 / math.pi
        self.size = size
        self.h = self.size * math.cos(30.0 * math.pi / 180.0)
        self.v = self.size * 0.5
        self.skip_x = 2.0 * self.h
        self.skip_y = 3.0 * self.v

    def rc2xy(self, r, c):
        ofs = self.h if r % 2L != 0 else 0
        x = c * self.skip_x + ofs
        y = r * self.skip_y
        return x, y

    def inside(self, px, py, cx, cy):
        qx = math.fabs(px - cx)
        qy = math.fabs(py - cy)
        return False if qx > self.h or qy > self.size else qx / self.h + qy / self.v <= 2.0

    def azimuth_to_degrees(self, px, py, cx, cy):
        az = math.atan2(px - cx, py - cy)  # reversed on purpose
        return az + self.two_pi * self.rad_to_deg if az < 0.0 else az * self.rad_to_deg

    def proceed_to_neighbor(self, px, py, cx, cy, old_r, old_c):
        deg = self.azimuth_to_degrees(px, py, cx, cy)
        if deg > 300.0:
            c = old_c if old_r % 2L != 0L else old_c - 1L
            r = old_r + 1L
        elif deg > 240.0:
            r = old_r
            c = old_c - 1L
        elif deg > 180.0:
            c = old_c - 1L if old_r % 2L != 0L else old_c
            r = old_r - 1L
        elif deg > 120.0:
            c = old_c + 1L if old_r % 2L != 0L else old_c
            r = old_r - 1L
        elif deg > 60.0:
            r = old_r
            c = old_c + 1L
        else:
            c = old_c + 1L if old_r % 2L != 0L else old_c
            r = old_r + 1L
        return r, c

    def xy2rc(self, px, py):
        r = long(math.floor(py / self.skip_y))
        c = long(math.floor(px / self.skip_x))
        cx, cy = self.rc2xy(r, c)
        while not self.inside(px, py, cx, cy):
            r, c = self.proceed_to_neighbor(px, py, cx, cy, r, c)
            cx, cy = self.rc2xy(r, c)
        return r, c
