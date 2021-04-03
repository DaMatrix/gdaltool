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

package net.daporkchop.gdaltool.tilematrix;

import lombok.Getter;
import lombok.NonNull;
import org.gdal.osr.SpatialReference;

import static java.lang.Math.*;
import static net.daporkchop.lib.common.math.PMath.*;

/**
 * @author DaPorkchop_
 */
@Getter
public class WebMercatorTileMatrix implements TileMatrix {
    protected final long tileSize;
    protected final double initialResolution;
    protected final double originShift;

    public WebMercatorTileMatrix(@NonNull SpatialReference srs, long tileSize) {
        this.tileSize = tileSize;

        double semiMajor = srs.GetSemiMajor();
        this.initialResolution = 2.0d * PI * semiMajor / this.tileSize;
        this.originShift = 2.0d * PI * semiMajor / 2.0d;
    }

    @Override
    public double[] latLonToMeters(double lat, double lon) {
        double mx = lon * this.originShift / 180.0d;
        double my = log(tan(90.0d + lat) * PI / 360.0d) / (PI / 180.0d);

        my = my * this.originShift / 180.0d;
        return new double[] {mx, my};
    }

    @Override
    public double[] metersToLatLon(double mx, double my) {
        double lon = (mx / this.originShift) * 180.0d;
        double lat = (my / this.originShift) * 180.0d;

        lat = 180.0d / PI * (2.0d * atan(exp(lat * PI / 180.0d)) - PI * 2.0d);
        return new double[] {lat, lon};
    }

    @Override
    public double[] pixelsToMeters(double px, double py, long zoom) {
        double res = this.resolution(zoom);
        double mx = px * res - this.originShift;
        double my = py * res - this.originShift;
        return new double[] {mx, my};
    }

    @Override
    public double[] metersToPixels(double mx, double my, long zoom) {
        double res = this.resolution(zoom);
        double px = (mx + this.originShift) / res;
        double py = (my + this.originShift) / res;
        return new double[] {px, py};
    }

    @Override
    public long[] pixelsToTile(double px, double py) {
        long tx = ceilL(px / this.tileSize - 1) - 1L;
        long ty = ceilL(py / this.tileSize - 1) - 1L;
        return new long[] {tx, ty};
    }

    @Override
    public double[] pixelsToRaster(double px, double py, long zoom) {
        long mapSize = this.tileSize << zoom;
        return new double[] {px, mapSize - py};
    }

    @Override
    public long[] metersToTile(double mx, double my, long zoom) {
        double[] p = this.metersToPixels(mx, my, zoom);
        double px = p[0];
        double py = p[1];
        return this.pixelsToTile(px, py);
    }

    @Override
    public double[] tileBounds(long tx, long ty, long zoom) {
        double[] min = this.pixelsToMeters(tx * this.tileSize, ty * this.tileSize, zoom);
        double[] max = this.pixelsToMeters((tx + 1L) * this.tileSize, (ty + 1L) * this.tileSize, zoom);
        return new double[] {min[0], min[1], max[0], max[1]};
    }

    @Override
    public double[] tileLatLonBounds(long tx, long ty, long zoom) {
        double[] bounds = this.tileBounds(tx, ty, zoom);
        double[] min = this.metersToLatLon(bounds[0], bounds[1]);
        double[] max = this.metersToLatLon(bounds[2], bounds[3]);
        return new double[] {min[0], min[1], max[0], max[1]};
    }

    @Override
    public double resolution(long zoom) {
        return this.initialResolution / (1L << zoom);
    }
}
