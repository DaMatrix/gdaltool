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

import lombok.NonNull;
import net.daporkchop.gdaltool.util.geom.Bounds2d;
import net.daporkchop.gdaltool.util.geom.GeoBounds2d;
import net.daporkchop.gdaltool.util.geom.GeoPoint2d;
import net.daporkchop.gdaltool.util.geom.Point2d;
import net.daporkchop.gdaltool.util.geom.Point2l;

/**
 * @author DaPorkchop_
 */
public interface TileMatrix {
    int MAX_ZOOM_LEVEL = 32;

    /**
     * @return the size of a tile, in pixels
     */
    int tileSize();

    /**
     * Converts given lat/lon in WGS84 Datum to XY in Spherical Mercator EPSG:3857
     */
    double[] latLonToMeters(double lat, double lon);

    /**
     * Converts XY point from Spherical Mercator EPSG:3857 to lat/lon in WGS84 Datum
     */
    GeoPoint2d metersToLatLon(@NonNull Point2d m);

    /**
     * Converts pixel coordinates in given zoom level of pyramid to EPSG:3857
     */
    double[] pixelsToMeters(double px, double py, int zoom);

    /**
     * Converts EPSG:3857 to pyramid pixel coordinates in given zoom level
     */
    Point2d metersToPixels(@NonNull Point2d m, int zoom);

    /**
     * Returns a tile covering region in given pixel coordinates
     */
    Point2l pixelsToTile(@NonNull Point2d p);

    /**
     * Move the origin of pixel coordinates to top-left corner
     */
    double[] pixelsToRaster(double px, double py, int zoom);

    /**
     * Returns tile for given mercator coordinates
     */
    Point2l metersToTile(@NonNull Point2d m, int zoom);

    /**
     * Returns bounds of the given tile in EPSG:3857 coordinates
     */
    Bounds2d tileBounds(@NonNull Point2l t, int zoom);

    /**
     * Returns bounds of the given tile in latitude/longitude using WGS84 datum
     */
    GeoBounds2d tileLatLonBounds(@NonNull Point2l t, int zoom);

    /**
     * Resolution (meters/pixel) for given zoom level (measured at Equator)
     */
    double resolution(int zoom);

    /**
     * Maximal scaledown zoom of the pyramid closest to the pixelSize.
     */
    default int zoomForPixelSize(double pixelSize) {
        for (int zoom = 0; zoom < MAX_ZOOM_LEVEL; zoom++) {
            if (pixelSize > this.resolution(zoom)) {
                return zoom;
            }
        }
        return MAX_ZOOM_LEVEL;
    }
}
