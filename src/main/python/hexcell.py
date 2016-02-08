import math


class HexCell:
    def __init__(self, size):
        self.xy = []
        for i in range(7):
            angle = math.pi * ((i % 6) + 0.5) / 3.0
            x = size * math.cos(angle)
            y = size * math.sin(angle)
            self.xy.append((x, y))

    def to_shape(self, cx, cy):
        return [[cx + x, cy + y] for (x, y) in self.xy]
