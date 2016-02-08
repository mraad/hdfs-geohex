import csv
import os
import unittest

from hexgrid import HexGrid


class HexGridTest(unittest.TestCase):
    def testHexGrid(self):
        hg = HexGrid(0.001)
        head, tail = os.path.split(os.path.abspath(__file__))
        head, tail = os.path.split(head)
        filename = os.path.join(head, 'resources', 'hex.csv')
        with open(filename, 'rb') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                px = float(row[0])
                py = float(row[1])
                er = long(row[2])
                ec = long(row[3])
                rr, rc = hg.xy2rc(px, py)
                self.assertEquals(er, rr)
                self.assertEquals(ec, rc)


if __name__ == '__main__':
    unittest.main()
