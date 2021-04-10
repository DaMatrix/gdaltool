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
import net.daporkchop.gdaltool.util.geom.Bounds2d;
import net.daporkchop.gdaltool.util.geom.Point2d;
import net.daporkchop.gdaltool.util.geom.Point2l;
import org.gdal.osr.SpatialReference;

import static java.lang.Math.*;
import static net.daporkchop.lib.common.math.PMath.*;

/**
 * @author DaPorkchop_
 */
@Getter
public class WGS84TileMatrix implements TileMatrix {
    protected final int tileSize;
    protected final double initialResolution;
    protected final double originShift;

    public WGS84TileMatrix(@NonNull SpatialReference srs, int tileSize) {
        this.tileSize = tileSize;

        this.initialResolution = 360.0d / this.tileSize;
        this.originShift = 360.0d / 2.0d;
    }

    @Override
    public double[] pixelsToMeters(double px, double py, int zoom) {
        double res = this.resolution(zoom);
        double mx = px * res - this.originShift;
        double my = py * res - this.originShift;
        return new double[]{ mx, my };
    }

    @Override
    public Point2d metersToPixels(@NonNull Point2d m, int zoom) {
        double res = this.resolution(zoom);
        double px = (m.x() + this.originShift) / res;
        double py = (m.y() + this.originShift) / res;
        return new Point2d(px, py);
    }

    @Override
    public Point2l pixelsToTile(@NonNull Point2d p) {
        long tx = ceilL(p.x() / this.tileSize) - 1L;
        long ty = ceilL(p.y() / this.tileSize) - 1L;
        return new Point2l(tx, ty);
    }

    @Override
    public Point2l metersToTile(@NonNull Point2d m, int zoom) {
        return this.pixelsToTile(this.metersToPixels(m, zoom));
    }

    @Override
    public Bounds2d tileBounds(@NonNull Point2l t, int zoom) {
        double[] min = this.pixelsToMeters(t.x() * this.tileSize, t.y() * this.tileSize, zoom);
        double[] max = this.pixelsToMeters((t.x() + 1L) * this.tileSize, (t.y() + 1L) * this.tileSize, zoom);
        return new Bounds2d(min[0], max[0], min[1], max[1]);
    }

    @Override
    public double resolution(int zoom) {
        return this.initialResolution / (1L << zoom);
    }
}
