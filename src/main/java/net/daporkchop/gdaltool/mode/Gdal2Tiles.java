/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2020-2021 DaPorkchop_
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software
 * is furnished to do so, subject to the following conditions:
 *
 * Any persons and/or organizations using this software must include the above copyright notice and this permission notice,
 * provide sufficient credit to the original authors of the project (IE: DaPorkchop_), as well as provide a link to the original project.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

package net.daporkchop.gdaltool.mode;

import lombok.Setter;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconst;
import org.gdal.osr.CoordinateTransformation;
import org.gdal.osr.SpatialReference;
import org.gdal.osr.osr;

import java.nio.file.Path;
import java.util.Arrays;

import static java.lang.Math.*;
import static net.daporkchop.lib.common.math.PMath.*;

/**
 * @author DaPorkchop_
 */
@Setter
public class Gdal2Tiles {
    protected Path src;
    protected Path dst;

    public void run() {
        final int TILE_SIZE = 256;
        final double INITIAL_RESOLUTION = 2.0d * PI * 6378137.0d / TILE_SIZE;
        final double ORIGIN_SHIFT = 2.0d * PI * 6378137.0d / 2.0d;

        Dataset _s = gdal.Open(this.src.toString());
        int sw = _s.getRasterXSize();
        int sh = _s.GetRasterYSize();
        double[] st = _s.GetGeoTransform();
        String s_srsWkt = _s.GetProjection();

        SpatialReference s_srs = new SpatialReference(s_srsWkt);
        SpatialReference g_srs = s_srs.CloneGeogCS();

        SpatialReference t_srs = g_srs.CloneGeogCS();
        t_srs.SetProjection(osr.SRS_PT_MERCATOR_1SP);
        t_srs.SetProjParm(osr.SRS_PP_CENTRAL_MERIDIAN, 0.0d);
        t_srs.SetProjParm(osr.SRS_PP_SCALE_FACTOR, 1.0d);
        t_srs.SetProjParm(osr.SRS_PP_FALSE_EASTING, 0.0d);
        t_srs.SetProjParm(osr.SRS_PP_FALSE_NORTHING, 0.0d);
        t_srs.SetLinearUnits("metre", 1.0d);
        t_srs.GetAxisName("X", osr.OAO_East);
        t_srs.GetAxisName("Y", osr.OAO_North);
        t_srs = s_srs;
        assert t_srs.Validate() == 0;
        String t_srsWkt = t_srs.ExportToWkt();

        System.out.println("Source projection: " + s_srs);
        System.out.println("Target projection: " + t_srs);

        Dataset _t = gdal.AutoCreateWarpedVRT(_s, s_srsWkt, t_srsWkt, gdalconst.GRA_CubicSpline);
        int tw = _t.GetRasterXSize();
        int th = _t.GetRasterYSize();
        double[] tt = _t.GetGeoTransform();

        _s.delete();
        _t.delete();

        double pixelSize = tt[1] * max(tw, th) / TILE_SIZE;
        int maxZoom;
        for (int zoom = 0;; zoom++) {
            if (pixelSize > INITIAL_RESOLUTION / (1 << zoom)) {
                maxZoom = max(0, zoom - 1);
                break;
            }
        }
        double resolution = INITIAL_RESOLUTION / (1 << maxZoom);

        CoordinateTransformation g2s = CoordinateTransformation.CreateCoordinateTransformation(g_srs, s_srs);
        double[] corner00 = g2s.TransformPoint(-180.0d, -90.0d);

        ThreadLocal<Dataset> tlSrcDataset = ThreadLocal.withInitial(() -> {
            Dataset dataset = gdal.Open(this.src.toString());
            Dataset vrt = gdal.AutoCreateWarpedVRT(dataset, s_srsWkt, t_srsWkt, gdalconst.GRA_CubicSpline);
            dataset.delete();
            return vrt;
        });

        t_srs.delete();
        s_srs.delete();
    }
}
