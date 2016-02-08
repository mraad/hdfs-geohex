#
# Spark job to bin data in WebMercator Spatial Reference
# The bin is a hexagon with a width of 100 meters
#
from pyspark import SparkContext

from hexgrid import HexGrid
from mercator import Mercator


def line_to_row_col(line, hg):
    splits = line.split(',')
    try:
        lon = float(splits[10])
        lat = float(splits[11])
        x, y = Mercator.to_web_mercator(lon, lat)
        rc = hg.xy2rc(x, y)
        return rc, 1
    except:
        return (0, 0), -1


if __name__ == "__main__":
    hg = HexGrid(100)
    sc = SparkContext()
    sc.textFile("hdfs:///trips"). \
        map(lambda line: line_to_row_col(line, hg)). \
        filter(lambda (rowcol, count): count > 0). \
        reduceByKey(lambda a, b: a + b). \
        filter(lambda (rowcol, count): count > 10). \
        map(lambda ((row, col), count): "{0},{1},{2}".format(row, col, count)). \
        saveAsTextFile("hdfs:///tmp/hex")
