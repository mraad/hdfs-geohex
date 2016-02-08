import math
import os
import re
import sys

import arcpy
import requests


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


class WebHDFSRes(object):
    def __init__(self, res):
        self.res = res

    def __enter__(self):
        return self.res

    def __exit__(self, exception_type, exception_value, traceback):
        self.res.close()


class WebHDFS(object):
    def __init__(self, host, user, port=50070):
        self.host = host
        self.port = port
        self.user = user

    def create_file(self, fc, sep, hdfs_path):
        description = arcpy.Describe(fc)
        field_names = [field.name for field in description.fields]
        shape_name = description.shapeFieldName
        field_names.remove(shape_name)
        field_names.append(shape_name + "@WKT")

        sep = "\t" if sep == "tab" else sep[0]

        result = arcpy.management.GetCount(fc)
        max_range = int(result.getOutput(0))
        arcpy.SetProgressor("step", "Exporting...", 0, max_range, 1)

        def gen_data():
            with arcpy.da.SearchCursor(fc, field_names) as cursor:
                arr = []
                cnt = 0
                inc = 0
                for row in cursor:
                    cnt += 1
                    inc += 1
                    arr.append(sep.join([str(r) for r in row]))
                    if inc == 5000:
                        inc = 0
                        arr.append("")
                        yield "\n".join(arr)
                        arr = []
                        arcpy.SetProgressorPosition(cnt)
                if len(arr) > 0:
                    arr.append("")
                    yield "\n".join(arr)

        params = {"op": "CREATE", "user.name": self.user, "buffersize": 1024 * 1024}
        url = "http://{}:{}/webhdfs/v1{}".format(self.host, self.port, hdfs_path)
        with WebHDFSRes(requests.put(url, params=params, allow_redirects=False)) as resLoc:
            if resLoc.status_code == 307:
                location = resLoc.headers["Location"]
                with WebHDFSRes(requests.put(location, data=gen_data())) as resPut:
                    if resPut.status_code != 201:
                        arcpy.AddError("Cannot put feature class. Error code = {}".format(resLoc.status_code))
            else:
                arcpy.AddError("Cannot get HDFS location. Error code = {}".format(resLoc.status_code))

        arcpy.ResetProgressor()
        return

    def open(self, hdfs_path, offset=-1, length=-1, buffer_size=-1):
        # arcpy.AddMessage(hdfs_path)
        params = {"op": "OPEN", "user.name": self.user}
        if offset > 0:
            params["offset"] = offset
        if length > 0:
            params["length"] = length
        if buffer_size > 0:
            params["buffersize"] = buffer_size
        url = "http://{}:{}/webhdfs/v1{}".format(self.host, self.port, hdfs_path)
        return WebHDFSRes(requests.get(url, params=params, stream=True))

    def list_status(self, hdfs_path, suffix_re="*"):
        files = []
        prog = re.compile(suffix_re)
        params = {"op": "LISTSTATUS", "user.name": self.user}
        url = "http://{}:{}/webhdfs/v1{}".format(self.host, self.port, hdfs_path)
        with WebHDFSRes(requests.get(url, params=params)) as res:
            doc = res.json()
            for i in doc["FileStatuses"]["FileStatus"]:
                path_suffix = i["pathSuffix"]
                if prog.match(path_suffix):
                    files.append("{}/{}".format(hdfs_path, path_suffix))
        return files


class Toolbox(object):
    def __init__(self):
        self.label = "WebHDFSToolbox"
        self.alias = "Web HDFS Toolbox"
        self.tools = [TripTool, DensityTool, HexTool, ExportToHDFSTool]


class HexTool(object):
    def __init__(self):
        self.label = "ImportHexCells"
        self.description = "WebHDFS tool to import hex row,col,population"
        self.canRunInBackground = True

    def getParameterInfo(self):
        param_fc = arcpy.Parameter(
                name="out_fc",
                displayName="out_fc",
                direction="Output",
                datatype="Feature Layer",
                parameterType="Derived")
        head, tail = os.path.split(os.path.abspath(__file__))
        param_fc.symbology = os.path.join(head, "HexCells.lyr")

        param_name = arcpy.Parameter(
                name="in_name",
                displayName="Name",
                direction="Input",
                datatype="GPString",
                parameterType="Required")
        param_name.value = "HexCells"

        param_host = arcpy.Parameter(
                name="in_host",
                displayName="HDFS Host",
                direction="Input",
                datatype="GPString",
                parameterType="Required")
        param_host.value = "sandbox"

        param_user = arcpy.Parameter(
                name="in_user",
                displayName="User name",
                direction="Input",
                datatype="GPString",
                parameterType="Required")
        param_user.value = "root"

        param_path = arcpy.Parameter(
                name="in_path",
                displayName="HDFS Path",
                direction="Input",
                datatype="GPString",
                parameterType="Required")
        param_path.value = "/tmp/rowcol"

        param_file = arcpy.Parameter(
                name="in_file",
                displayName="HDFS File(s)",
                direction="Input",
                datatype="GPString",
                parameterType="Required")
        param_file.value = "part.*"

        param_spref = arcpy.Parameter(name="in_spref",
                                      displayName="Spatial Reference",
                                      direction="Input",
                                      datatype="GPSpatialReference",
                                      parameterType="Required")

        param_size = arcpy.Parameter(name="in_size",
                                     displayName="Hex Size",
                                     direction="Input",
                                     datatype="GPLong",
                                     parameterType="Required")
        param_size.value = 100

        return [param_fc, param_name, param_host, param_user, param_path, param_file, param_spref, param_size]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        name = parameters[1].value
        host = parameters[2].value
        user = parameters[3].value
        path = parameters[4].value
        fext = parameters[5].value
        sref = parameters[6].value
        size = parameters[7].value

        in_memory = False
        if in_memory:
            ws = "in_memory"
            fc = ws + "/" + name
        else:
            fc = os.path.join(arcpy.env.scratchGDB, name)
            ws = os.path.dirname(fc)

        if arcpy.Exists(fc):
            arcpy.management.Delete(fc)

        arcpy.management.CreateFeatureclass(ws, name, "POLYGON", spatial_reference=sref)
        arcpy.management.AddField(fc, "POPULATION", "LONG")

        with arcpy.da.InsertCursor(fc, ["SHAPE@", "POPULATION"]) as cursor:
            webhdfs = WebHDFS(host, user)
            for path in webhdfs.list_status(path, fext):
                with webhdfs.open(hdfs_path=path, buffer_size=1024 * 1024) as res:
                    hex_grid = HexGrid(size=size)
                    hex_cell = HexCell(size=size)
                    for line in res.iter_lines(chunk_size=1024 * 1024):
                        row_txt, col_txt, pop_txt = line.split(",")
                        row = float(row_txt)
                        col = float(col_txt)
                        pop = long(pop_txt)
                        x, y = hex_grid.rc2xy(row, col)
                        cursor.insertRow((hex_cell.to_shape(x, y), pop))

        parameters[0].value = fc
        return


class DensityTool(object):
    def __init__(self):
        self.label = "ImportPointDensity"
        self.description = "WebHDFS tool to import x,y,population"
        self.canRunInBackground = True

    def getParameterInfo(self):
        param_fc = arcpy.Parameter(
                name="out_fc",
                displayName="out_fc",
                direction="Output",
                datatype="Feature Layer",
                parameterType="Derived")

        param_name = arcpy.Parameter(
                name="in_name",
                displayName="Name",
                direction="Input",
                datatype="GPString",
                parameterType="Required")
        param_name.value = "DensityPoints"

        param_host = arcpy.Parameter(
                name="in_host",
                displayName="HDFS Host",
                direction="Input",
                datatype="GPString",
                parameterType="Required")
        param_host.value = "sandbox"

        param_user = arcpy.Parameter(
                name="in_user",
                displayName="User name",
                direction="Input",
                datatype="GPString",
                parameterType="Required")
        param_user.value = "root"

        param_path = arcpy.Parameter(
                name="in_path",
                displayName="HDFS Path",
                direction="Input",
                datatype="GPString",
                parameterType="Required")
        param_path.value = "/tmp/rowcol"

        param_file = arcpy.Parameter(
                name="in_file",
                displayName="HDFS File(s)",
                direction="Input",
                datatype="GPString",
                parameterType="Required")
        param_file.value = "part.*"

        param_spref = arcpy.Parameter(name="in_spref",
                                      displayName="Spatial Reference",
                                      direction="Input",
                                      datatype="GPSpatialReference",
                                      parameterType="Required")

        return [param_fc, param_name, param_host, param_user, param_path, param_file, param_spref]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        name = parameters[1].value
        host = parameters[2].value
        user = parameters[3].value
        path = parameters[4].value
        fext = parameters[5].value
        sref = parameters[6].value

        in_memory = False
        if in_memory:
            ws = "in_memory"
            fc = ws + "/" + name
        else:
            fc = os.path.join(arcpy.env.scratchGDB, name)
            ws = os.path.dirname(fc)

        if arcpy.Exists(fc):
            arcpy.management.Delete(fc)

        arcpy.management.CreateFeatureclass(ws, name, "POINT", spatial_reference=sref)
        arcpy.management.AddField(fc, "POPULATION", "LONG")

        with arcpy.da.InsertCursor(fc, ["SHAPE@XY", "POPULATION"]) as cursor:
            webhdfs = WebHDFS(host, user)
            for path in webhdfs.list_status(path, fext):
                with webhdfs.open(hdfs_path=path, buffer_size=1024 * 1024) as res:
                    for line in res.iter_lines(chunk_size=1024 * 1024):
                        lon_txt, lat_txt, pop_txt = line.split(",")
                        lon = float(lon_txt)
                        lat = float(lat_txt)
                        pop = long(pop_txt)
                        cursor.insertRow(((lon, lat), pop))

        parameters[0].value = fc
        return


class TripTool(object):
    def __init__(self):
        self.label = "ImportTrips"
        self.description = "WebHDFS tool to import trips"
        self.canRunInBackground = True

    def getParameterInfo(self):
        param_fc = arcpy.Parameter(
                name="out_fc",
                displayName="out_fc",
                direction="Output",
                datatype="Feature Layer",
                parameterType="Derived")

        param_name = arcpy.Parameter(
                name="in_name",
                displayName="Name",
                direction="Input",
                datatype="GPString",
                parameterType="Required")
        param_name.value = "PickupPoints"

        param_host = arcpy.Parameter(
                name="in_host",
                displayName="HDFS Host",
                direction="Input",
                datatype="GPString",
                parameterType="Required")
        param_host.value = "sandbox"

        param_user = arcpy.Parameter(
                name="in_user",
                displayName="User name",
                direction="Input",
                datatype="GPString",
                parameterType="Required")
        param_user.value = "root"

        param_path = arcpy.Parameter(
                name="in_path",
                displayName="HDFS Path",
                direction="Input",
                datatype="GPString",
                parameterType="Required")
        param_path.value = "/trips"

        param_file = arcpy.Parameter(
                name="in_file",
                displayName="HDFS File(s)",
                direction="Input",
                datatype="GPString",
                parameterType="Required")
        param_file.value = "trips.*"

        return [param_fc, param_name, param_host, param_user, param_path, param_file]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        reload(sys)
        sys.setdefaultencoding("utf8")

        name = parameters[1].value
        host = parameters[2].value
        user = parameters[3].value
        path = parameters[4].value
        fext = parameters[5].value

        in_memory = False
        if in_memory:
            ws = "in_memory"
            fc = ws + "/" + name
        else:
            fc = os.path.join(arcpy.env.scratchGDB, name)
            ws = os.path.dirname(fc)

        if arcpy.Exists(fc):
            arcpy.management.Delete(fc)

        sp_ref = arcpy.SpatialReference(4326)
        arcpy.management.CreateFeatureclass(ws, name, "POINT", spatial_reference=sp_ref)
        arcpy.management.AddField(fc, "DATETIME", "TEXT", field_length=32)
        arcpy.management.AddField(fc, "PASSENGERS", "LONG")

        with arcpy.da.InsertCursor(fc, ["SHAPE@XY", "DATETIME", "PASSENGERS"]) as cursor:
            webhdfs = WebHDFS(host, user)
            for path in webhdfs.list_status(path, fext):
                arcpy.AddMessage(path)
                with webhdfs.open(hdfs_path=path, buffer_size=1024 * 1024) as res:
                    line_no = 0
                    for line in res.iter_lines(chunk_size=1024 * 1024):
                        line_no += 1
                        if line_no > 1:
                            tokens = line.split(",")
                            if len(tokens) > 11:
                                datetime = tokens[5]
                                passengers = int(tokens[7])
                                lon = float(tokens[10])
                                lat = float(tokens[11])
                                if -74.255 < lon < -73.608 and 40.618 < lat < 40.937:
                                    cursor.insertRow(((lon, lat), datetime, passengers))

        parameters[0].value = fc
        return


class ExportToHDFSTool(object):
    def __init__(self):
        self.label = "ExportToHDFS"
        self.description = """
        Export a feature class to HDFS in text format, where each feature is a row terminated by a line feed
        and each feature attribute is terminated by a tab.  The shape of the feature is stored in WKT format.
        """
        self.canRunInBackground = True

    def getParameterInfo(self):
        param_fc = arcpy.Parameter(name="in_fc",
                                   displayName="Input Feature Class",
                                   direction="Input",
                                   datatype="Table View",
                                   parameterType="Required")

        param_sep = arcpy.Parameter(name="in_sep",
                                    displayName="Output Field Separator",
                                    direction="Input",
                                    datatype="String",
                                    parameterType="Required")
        param_sep.value = "tab"

        param_host = arcpy.Parameter(name="in_host",
                                     displayName="HDFS Host",
                                     direction="Input",
                                     datatype="String",
                                     parameterType="Required")
        param_host.value = "sandbox"

        param_user = arcpy.Parameter(name="in_user",
                                     displayName="HDFS User",
                                     direction="Input",
                                     datatype="String",
                                     parameterType="Required")
        param_user.value = "root"

        param_path = arcpy.Parameter(name="in_path",
                                     displayName="Output HDFS Path",
                                     direction="Input",
                                     datatype="String",
                                     parameterType="Required")
        param_path.value = "/user/root"

        return [param_fc, param_sep, param_host, param_user, param_path]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        reload(sys)
        sys.setdefaultencoding("utf8")

        fc = parameters[0].valueAsText
        sep = parameters[1].valueAsText
        hdfs_host = parameters[2].valueAsText
        hdfs_user = parameters[3].valueAsText
        hdfs_path = parameters[4].valueAsText

        webhdfs = WebHDFS(hdfs_host, hdfs_user)
        webhdfs.create_file(fc, sep, hdfs_path)
        return
