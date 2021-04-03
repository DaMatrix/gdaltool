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

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.ToString;
import net.daporkchop.gdaltool.tilematrix.TileMatrix;
import net.daporkchop.gdaltool.tilematrix.WebMercatorTileMatrix;
import net.daporkchop.gdaltool.util.Bounds2d;
import net.daporkchop.gdaltool.util.Bounds2l;
import net.daporkchop.gdaltool.util.Point2l;
import net.daporkchop.gdaltool.util.Resampling;
import net.daporkchop.lib.unsafe.PUnsafe;
import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.Driver;
import org.gdal.gdal.gdal;
import org.gdal.osr.SpatialReference;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.lang.Math.*;
import static net.daporkchop.gdaltool.util.GdalHelper.*;
import static net.daporkchop.lib.common.util.PValidation.*;
import static org.gdal.gdalconst.gdalconst.*;
import static org.gdal.osr.osr.*;

/**
 * @author DaPorkchop_
 */
@Setter
public class Gdal2Tiles {
    protected static GeoQuery geoQuery(@NonNull double[] gt, int rasterX, int rasterY, double ulx, double uly, double lrx, double lry) {
        int rx = (int) ((ulx - gt[0]) / gt[1] + 0.001d);
        int ry = (int) ((uly - gt[3]) / gt[5] + 0.001d);
        int rxSize = max(1, (int) ((lrx - ulx) / gt[1] + 0.5d));
        int rySize = max(1, (int) ((lry - uly) / gt[5] + 0.5d));

        int wx = 0;
        int wy = 0;
        int wxSize = rxSize;
        int wySize = rySize;

        if (rx < 0) {
            int rxShift = abs(rx);
            wx = (int) (wxSize * ((double) rxShift / rxSize));
            wxSize = wxSize - wx;
            rxSize = rxSize - (int) (rxSize * ((double) rxShift / rxSize));
            rx = 0;
        }
        if (rx + rxSize > rasterX) {
            wxSize = (int) (wySize * (((double) rasterX - rx) / rxSize));
            rxSize = rasterX - rx;
        }

        if (ry < 0) {
            int ryShift = abs(ry);
            wy = (int) (wySize * ((double) ryShift / rySize));
            wySize = wySize - wy;
            rySize = rySize - (int) (rySize * ((double) ryShift / rySize));
            ry = 0;
        }
        if (ry + rySize > rasterY) {
            wySize = (int) (wySize * (((double) rasterY - ry) / rySize));
            rySize = rasterY - ry;
        }

        return new GeoQuery(rx, ry, rxSize, rySize, wx, wy, wxSize, wySize);
    }

    protected final Dataset inputDataset;
    protected final ThreadLocal<Dataset> tlWarpedInputDataset;
    protected final int input_bands;
    protected final int input_dataType;
    protected final Double[] input_noDataValues;

    protected final Path outputFolder;
    protected final String outExt;
    protected final Driver outDrv;

    protected final Resampling resampling;
    protected final CuttingProfile profile;

    protected final SpatialReference in_srs;
    protected final SpatialReference out_srs;
    protected final Bounds2l[] tMinMax;
    protected TileMatrix tileMatrix;

    protected final int minZoom;
    protected final int maxZoom;

    protected final double[] out_gt;
    protected final int out_rasterXSize;
    protected final int out_rasterYSize;
    protected final Bounds2d out_bounds;

    protected final int tileSize;
    protected final boolean xyz;

    @SneakyThrows(IOException.class)
    public Gdal2Tiles(@NonNull Path inputFile, @NonNull Path outputFolder, @NonNull Options options) {
        this.tileSize = options.tileSize();
        this.resampling = options.resampling();
        this.profile = options.profile();
        this.xyz = options.xyz();

        this.outDrv = getDriverByName(options.tileDriver());

        this.outputFolder = Files.createDirectories(outputFolder);
        this.outExt = options.tileExt();

        int minZoom = options.minZoom();
        int maxZoom = options.maxZoom();
        checkArg(minZoom >> 31 == maxZoom >> 31, "minZoom and maxZoom must be either both set or both unset");
        checkArg(minZoom <= maxZoom, "minZoom (%d) must not be greater than maxZoom (%d)", minZoom, maxZoom);

        checkArg((this.inputDataset = gdal.Open(inputFile.toString(), GA_ReadOnly)) != null, "unable to open input file: \"%s\"", inputFile);
        checkArg(this.inputDataset.getRasterCount() != 0, "input file \"%s\" has no raster bands", inputFile);
        System.out.printf("input file: (%dP x %dL - %d bands)\n",
                this.inputDataset.getRasterXSize(), this.inputDataset.getRasterYSize(), this.inputDataset.getRasterCount());
        this.input_bands = this.inputDataset.GetRasterCount();
        this.input_dataType = this.inputDataset.GetRasterBand(1).GetRasterDataType();
        this.input_noDataValues = getNoDataValues(this.inputDataset);

        this.in_srs = setupInputSrs(this.inputDataset, options.s_srs());

        if (this.profile != CuttingProfile.RASTER) {
            checkArg(this.in_srs != null, "Input file has unknown SRS! Use '--s_srs EPSG:<xyz> or similar to provide source reference system.");
            checkArg(hasGeoReference(this.inputDataset), "There is no georeference - neither affine transformation (worldfile)\n"
                                                         + "nor GCPs. You can generate only 'raster' profile tiles.\n"
                                                         + "Either gdal2tiles with parameter -p 'raster' or use another GIS\n"
                                                         + "software for georeference e.g. gdal_transform -gcp / -a_ullr / -a_srs");
        }

        this.out_srs = this.profile.getSrsForInput(this.inputDataset, this.in_srs);

        Dataset warpedInputDataset = this.inputDataset;
        if (this.profile != CuttingProfile.RASTER) {
            if (!this.in_srs.ExportToProj4().equals(this.out_srs.ExportToProj4()) || this.inputDataset.GetGCPCount() != 0) {
                warpedInputDataset = gdal.AutoCreateWarpedVRT(this.inputDataset, this.in_srs.ExportToWkt(), this.out_srs.ExportToWkt(), this.resampling.gdal());
            }
        }
        System.out.printf("projected input file: (%dP x %dL - %d bands)\n",
                warpedInputDataset.getRasterXSize(), warpedInputDataset.getRasterYSize(), warpedInputDataset.getRasterCount());
        String tempFile = Files.createTempFile("tiles", ".vrt").toString();
        getDriverByName("VRT").CreateCopy(tempFile, warpedInputDataset);
        this.tlWarpedInputDataset = ThreadLocal.withInitial(() -> gdal.Open(tempFile, GA_ReadOnly));

        this.out_gt = warpedInputDataset.GetGeoTransform();
        this.out_rasterXSize = warpedInputDataset.getRasterXSize();
        this.out_rasterYSize = warpedInputDataset.getRasterYSize();
        this.out_bounds = new Bounds2d(
                this.out_gt[0],
                this.out_gt[0] + warpedInputDataset.GetRasterXSize() * this.out_gt[1],
                this.out_gt[3],
                this.out_gt[3] + warpedInputDataset.GetRasterYSize() * this.out_gt[1]);

        CuttingProfile.ProjectionData projectionData = this.profile.getProjectionData(this.tileSize, this.in_srs, this.out_srs, this.inputDataset, warpedInputDataset, this.out_bounds);
        if (minZoom < 0) {
            minZoom = 0;
            maxZoom = projectionData.defaultMaxZoom;
        }
        this.tileMatrix = projectionData.tileMatrix;
        this.tMinMax = projectionData.tMinMax;

        if (warpedInputDataset != this.inputDataset) {
            warpedInputDataset.delete();
        }

        this.minZoom = minZoom;
        this.maxZoom = maxZoom;
    }

    protected Dataset createMemTile() {
        Dataset ds = MEM_DRIVER.Create("", this.tileSize, this.tileSize, this.input_bands, this.input_dataType);

        for (int b = 1; b <= this.input_bands; b++) {
            Band band = ds.GetRasterBand(b );
            Double noData = this.input_noDataValues[b - 1];
            if (noData != null) {
                band.SetNoDataValue(noData);
                band.Fill(noData);
            } else {
                band.DeleteNoDataValue();
            }
        }

        return ds;
    }

    protected Stream<TileDetail> getTilePositions(int zoom) {
        Bounds2l tBounds = this.tMinMax[zoom];
        return LongStream.rangeClosed(tBounds.minY(), tBounds.maxY()).boxed()
                .flatMap(_ty -> {
                    long ty = _ty; //unbox lol
                    return LongStream.rangeClosed(tBounds.minX(), tBounds.maxX())
                            .mapToObj(tx -> {
                                long yTile = this.xyz ? ((1L << zoom) - 1L) - ty : ty;
                                Path tileFile = this.tileFile(zoom, tx, yTile);
                                Bounds2d b = this.tileMatrix.tileBounds(new Point2l(tx, ty), zoom);

                                GeoQuery q = geoQuery(this.out_gt, this.out_rasterXSize, this.out_rasterYSize, b.minX(), b.maxY(), b.maxX(), b.minY());
                                return new TileDetail(tx, yTile, zoom, q.rx, q.ry, q.rxSize, q.rySize, q.wx, q.wy, q.wxSize, q.wySize, tileFile);
                            });
                });
    }

    @SneakyThrows(IOException.class)
    protected void createBaseTile(@NonNull TileDetail t) {
        Dataset tile = this.createMemTile();
        Dataset input = this.tlWarpedInputDataset.get();

        if (t.rxSize != 0 && t.rySize != 0 && t.wxSize != 0 && t.wySize != 0) {
            for (int b = 1; b <= this.input_bands; b++) {
                ByteBuffer data = input.GetRasterBand(b).ReadRaster_Direct(t.rx, t.ry, t.rxSize, t.rySize, t.wxSize, t.wySize, this.input_dataType);
                if (data != null) {
                    tile.GetRasterBand(b).WriteRaster_Direct(t.wx, t.wy, t.wxSize, t.wySize, this.input_dataType, data);
                    PUnsafe.pork_releaseBuffer(data);
                }
            }
        }

        Files.createDirectories(t.tileFile.getParent());
        this.outDrv.CreateCopy(t.tileFile.toString(), tile, 0);

        tile.delete();
    }

    public void run() {
        this.getTilePositions(this.maxZoom).forEach(this::createBaseTile);
    }

    protected Path tileFile(int zoom, long tx, long ty) {
        return this.outputFolder.resolve(String.valueOf(zoom)).resolve(String.valueOf(tx)).resolve(ty + "." + this.outExt);
    }

    public enum CuttingProfile {
        MERCATOR {
            @Override
            public SpatialReference getSrsForInput(@NonNull Dataset inputDataset, SpatialReference s_srs) {
                //TODO: don't assume we're on earth
                /*SpatialReference t_srs = s_srs.CloneGeogCS();
                t_srs.SetProjection(osr.SRS_PT_MERCATOR_1SP);
                t_srs.SetProjParm(osr.SRS_PP_CENTRAL_MERIDIAN, 0.0d);
                t_srs.SetProjParm(osr.SRS_PP_SCALE_FACTOR, 1.0d);
                t_srs.SetProjParm(osr.SRS_PP_FALSE_EASTING, 0.0d);
                t_srs.SetProjParm(osr.SRS_PP_FALSE_NORTHING, 0.0d);
                t_srs.SetLinearUnits("metre", 1.0d);
                t_srs.GetAxisName("X", osr.OAO_East);
                t_srs.GetAxisName("Y", osr.OAO_North);
                assert t_srs.Validate() == 0;*/

                SpatialReference t_srs = new SpatialReference();
                t_srs.ImportFromEPSG(3857);
                t_srs.SetAxisMappingStrategy(OAMS_TRADITIONAL_GIS_ORDER);
                return t_srs;
            }

            @Override
            public ProjectionData getProjectionData(int tileSize, @NonNull SpatialReference s_srs, @NonNull SpatialReference t_srs, @NonNull Dataset inputDataset, @NonNull Dataset warpedInputDataset, @NonNull Bounds2d out_bounds) {
                ProjectionData data = new ProjectionData();
                data.tileMatrix = new WebMercatorTileMatrix(t_srs, tileSize);
                data.tMinMax = IntStream.range(0, TileMatrix.MAX_ZOOM_LEVEL)
                        .mapToObj(zoom -> {
                            Point2l tmin = data.tileMatrix.metersToTile(out_bounds.min(), zoom);
                            Point2l tmax = data.tileMatrix.metersToTile(out_bounds.max(), zoom);
                            return new Bounds2l(max(0L, tmin.x()), min((1L << zoom) - 1L, tmax.x()), max(0L, tmin.y()), min((1L << zoom) - 1L, tmax.y()));
                        })
                        .toArray(Bounds2l[]::new);
                data.defaultMaxZoom = data.tileMatrix.zoomForPixelSize(warpedInputDataset.GetGeoTransform()[1]);
                return data;
            }
        },
        RASTER {
            @Override
            public SpatialReference getSrsForInput(@NonNull Dataset inputDataset, SpatialReference s_srs) {
                return s_srs;
            }

            @Override
            public ProjectionData getProjectionData(int tileSize, @NonNull SpatialReference s_srs, @NonNull SpatialReference t_srs, @NonNull Dataset inputDataset, @NonNull Dataset warpedInputDataset, @NonNull Bounds2d out_bounds) {
                throw new UnsupportedOperationException();
            }
        };

        public abstract SpatialReference getSrsForInput(@NonNull Dataset inputDataset, SpatialReference s_srs);

        public abstract ProjectionData getProjectionData(int tileSize, @NonNull SpatialReference s_srs, @NonNull SpatialReference t_srs, @NonNull Dataset inputDataset, @NonNull Dataset warpedInputDataset, @NonNull Bounds2d out_bounds);

        public static class ProjectionData {
            public TileMatrix tileMatrix;

            public int defaultMaxZoom;
            public Bounds2l[] tMinMax;
        }
    }

    @RequiredArgsConstructor
    protected static class GeoQuery {
        public final int rx;
        public final int ry;
        public final int rxSize;
        public final int rySize;
        public final int wx;
        public final int wy;
        public final int wxSize;
        public final int wySize;
    }

    @RequiredArgsConstructor
    protected static class TileDetail {
        public final long tx;
        public final long ty;
        public final int tz;
        public final int rx;
        public final int ry;
        public final int rxSize;
        public final int rySize;
        public final int wx;
        public final int wy;
        public final int wxSize;
        public final int wySize;
        @NonNull
        public final Path tileFile;
    }

    @Getter
    @Setter
    @ToString
    @EqualsAndHashCode
    public static class Options {
        protected int tileSize = 256;

        protected int minZoom = -1;
        protected int maxZoom = -1;

        @NonNull
        protected String tileDriver = "GTiff";
        @NonNull
        protected String tileExt = "tiff";

        @NonNull
        protected Resampling resampling = Resampling.Average;

        protected String s_srs = null;

        @NonNull
        protected CuttingProfile profile = CuttingProfile.MERCATOR;

        protected boolean xyz = true;
    }
}
