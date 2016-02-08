#
# Spark job to bin data in WebMercator Spatial Reference
# The bin size is 100 meters
#
import math

from pyspark import SparkContext

from mercator import Mercator


def line_to_row_col(line):
    splits = line.split(',')
    try:
        lon = float(splits[10])
        lat = float(splits[11])
        x, y = Mercator.to_web_mercator(lon, lat)
        c = int(math.floor(x / 100))
        r = int(math.floor(y / 100))
        return (r, c), 1
    except:
        return (0, 0), -1


def row_col_to_xy(row, col, count):
    y = row * 100 + 50
    x = col * 100 + 50
    return "{0},{1},{2}".format(x, y, count)


if __name__ == "__main__":
    sc = SparkContext()
    sc.textFile("hdfs:///trips"). \
        map(lambda line: line_to_row_col(line)). \
        filter(lambda (rowcol, count): count > 0). \
        reduceByKey(lambda a, b: a + b). \
        filter(lambda (rowcol, count): count > 2). \
        map(lambda ((row, col), count): row_col_to_xy(row, col, count)). \
        saveAsTextFile("hdfs:///tmp/rowcol")
