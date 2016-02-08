#
# Spark job to bin data in WGS84 Spatial Reference
# The bin size is 0.001 degrees
#
import math

from pyspark import SparkContext


def line_to_row_col(line):
    splits = line.split(',')
    try:
        p_lon = float(splits[10])
        p_lat = float(splits[11])
        c = int(math.floor(p_lon / 0.001))
        r = int(math.floor(p_lat / 0.001))
        return (r, c), 1
    except:
        return (0, 0), -1


def row_col_to_xy(row, col, count):
    y = row * 0.001 + 0.0005
    x = col * 0.001 + 0.0005
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
