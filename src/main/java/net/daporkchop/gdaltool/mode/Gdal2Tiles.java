/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2020-2023 DaPorkchop_
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

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import net.daporkchop.gdaltool.tilematrix.RasterTileMatrix;
import net.daporkchop.gdaltool.tilematrix.TileMatrix;
import net.daporkchop.gdaltool.tilematrix.WGS84TileMatrix;
import net.daporkchop.gdaltool.tilematrix.WebMercatorTileMatrix;
import net.daporkchop.gdaltool.util.DataType;
import net.daporkchop.gdaltool.util.ProgressMonitor;
import net.daporkchop.gdaltool.util.Resampling;
import net.daporkchop.gdaltool.util.geom.Bounds2d;
import net.daporkchop.gdaltool.util.geom.Bounds2l;
import net.daporkchop.gdaltool.util.geom.Point2d;
import net.daporkchop.gdaltool.util.geom.Point2l;
import net.daporkchop.gdaltool.util.option.Arguments;
import net.daporkchop.gdaltool.util.option.Option;
import net.daporkchop.lib.unsafe.PUnsafe;
import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.Driver;
import org.gdal.gdal.gdal;
import org.gdal.osr.SpatialReference;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.Vector;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.RecursiveTask;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.lang.Math.*;
import static net.daporkchop.gdaltool.util.GdalHelper.*;
import static net.daporkchop.lib.common.math.PMath.*;
import static net.daporkchop.lib.common.util.PValidation.*;
import static org.gdal.gdalconst.gdalconst.*;
import static org.gdal.osr.osr.*;

/**
 * @author DaPorkchop_
 */
@Setter
public class Gdal2Tiles implements Mode {
    private static final Option<Integer> TILE_SIZE = Option.integer("-tileSize", 256, 1, Integer.MAX_VALUE);

    private static final Option<Integer> MIN_ZOOM = Option.integer("-minZoom", -1, 0, Integer.MAX_VALUE);
    private static final Option<Integer> MAX_ZOOM = Option.integer("-maxZoom", -1, 0, Integer.MAX_VALUE);
    private static final Option<Integer> CLAMP_MAX_ZOOM = Option.integer("-clampMaxZoom", -1, 0, Integer.MAX_VALUE);

    private static final Option<Double[]> A_NODATA = new Option.BaseOption<Double[]>("-a_nodata") {
        @Override
        public Double[] parse(@NonNull String word, @NonNull Iterator<String> itr) {
            return Stream.of(itr.next().split(","))
                    .map(v -> "none".equalsIgnoreCase(v) || "null".equalsIgnoreCase(v) ? null : Double.parseDouble(v))
                    .toArray(Double[]::new);
        }

        @Override
        public Double[] fallbackValue() {
            return null;
        }
    };

    private static final Option<String> TILE_DRIVER = Option.text("-tileDriver", "GTiff");
    private static final Option<String> TILE_EXT = Option.text("-tileExt", "tiff");

    private static final Option<Resampling> RESAMPLING = Option.ofEnum("-resampling", Resampling.class, Resampling.CubicSpline);
    private static final Option<DataType> DATA_TYPE = Option.ofEnum("-dataType", DataType.class, DataType.Default);

    private static final Option<Double> SCALE = Option.ofDouble("-scale", 1.0d);

    private static final Option<Vector<String>> OPTIONS = new Option.BaseOption<Vector<String>>("-options") {
        @Override
        public Vector<String> parse(@NonNull String word, @NonNull Iterator<String> itr) {
            return new Vector<>(Arrays.asList(itr.next().split(",")));
        }

        @Override
        public Vector<String> fallbackValue() {
            return new Vector<>();
        }
    };

    private static final Option<String> S_SRS = Option.text("-s_srs", null);

    private static final Option<CuttingProfile> CUTTING_PROFILE = Option.ofEnum("-cutting", CuttingProfile.class, CuttingProfile.MERCATOR);
    private static final Option<Boolean> XYZ = Option.flag("-xyz");

    protected static GeoQuery geoQuery(@NonNull double[] gt, int rasterX, int rasterY, int querySize, double ulx, double uly, double lrx, double lry) {
        int rx = (int) ((ulx - gt[0]) / gt[1] + 0.001d);
        int ry = (int) ((uly - gt[3]) / gt[5] + 0.001d);
        int rxSize = max(1, (int) ((lrx - ulx) / gt[1] + 0.5d));
        int rySize = max(1, (int) ((lry - uly) / gt[5] + 0.5d));

        int wx = 0;
        int wy = 0;
        int wxSize = querySize;
        int wySize = querySize;

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

    protected ForkJoinPool forkJoinPool;

    protected Dataset inputDataset;
    protected ThreadLocal<Dataset> tlWarpedInputDataset;
    protected ThreadLocal<ByteBuffer> tlBuffer;
    protected ByteBuffer emptyTileBuffer;
    protected int input_bands;
    protected DataType input_dataType;
    protected DataType output_dataType;
    protected int[] allBands;
    protected Double[] input_noDataValues;
    protected Double[] output_noDataValues;

    protected Path outputFolder;
    protected String outExt;
    protected Driver outDrv;

    protected Resampling resampling;
    protected CuttingProfile profile;

    protected SpatialReference in_srs;
    protected SpatialReference out_srs;
    protected String out_srsWkt;
    protected Bounds2l[] tMinMax;
    protected TileMatrix tileMatrix;

    protected int minZoom;
    protected int maxZoom;

    protected double[] out_gt;
    protected int out_rasterXSize;
    protected int out_rasterYSize;
    protected Bounds2d out_bounds;
    protected Vector<String> out_options;

    protected int tileSize;
    protected boolean xyz;

    protected double scale;

    @Override
    public void printUsage() {
        System.err.println("  gdal2tiles:");
        System.err.println("    Computes the bounds of a given dataset.");
        System.err.println();
        System.err.println("    Usage:");
        System.err.println("      gdal2tiles [options] <input_dataset> <output_directory>");
        System.err.println();
        System.err.println("    Options:");
        System.err.println("      --tileSize <size>           Sets the horizontal and vertical resolution of the generated tiles. Default: 256");
        System.err.println("      --minZoom <zoom>            Sets the minimum zoom level at which to generate tiles (inclusive). Default: 0");
        System.err.println("      --maxZoom <zoom>            Sets the maximum zoom level at which to generate tiles (inclusive). Default: automatic");
        System.err.println("      --clampMaxZoom <zoom>       Sets the maximum zoom level at which to generate tiles (inclusive). Mutually exclusive with --maxZoom. When this option is set, the maximum zoom level will be determined automatically, but will be no higher than this value.");
        System.err.println("      --a_nodata <nodata,...>     Defines the nodata value to be set for output tiles. Multiple comma-separated values may be given. Default: copied from input dataset");
        System.err.println("      --tileDriver <driver>       Sets the GDAL driver used for creating the output tiles. Default: GTiff");
        System.err.println("      --tileExt <ext>             Sets the filename extension appended to output tiles. Default: tiff");
        System.err.println("      --resampling <resampling>   Sets the resampling algorithm used when reprojecting/rescaling. Default: CubicSpline");
        System.err.println("      --dataType <data_type>      Sets the data type to be used in output tiles. Default: copied from input dataset");
        System.err.println("      --scale <scale>             A constant factor to multiply all values by before writing them to the output dataset. Default: 1.0");
        System.err.println("      --options <options,...>     Additional comma-separated dataset creation options to pass to the GDAL driver when creating the output tiles. Default: none");
        System.err.println("      --s_srs <SRS>               If present, overrides the dataset's SRS.");
        System.err.println("      --cutting <cutting>         Sets the cutting profile used to split the input dataset into square tiles. Default: MERCATOR");
        System.err.println("      --xyz                       If present, use a different y coordinate order for output filenames.");
    }

    @Override
    public Arguments arguments() {
        return new Arguments(true, true, TILE_SIZE, MIN_ZOOM, MAX_ZOOM, CLAMP_MAX_ZOOM, A_NODATA, TILE_DRIVER, TILE_EXT, RESAMPLING, DATA_TYPE, SCALE, OPTIONS, S_SRS, CUTTING_PROFILE, XYZ);
    }

    @Override
    public String name() {
        return "gdal2tiles";
    }

    @Override
    @SneakyThrows(IOException.class)
    public void run(@NonNull Arguments args) {
        this.tileSize = args.get(TILE_SIZE);
        this.resampling = args.get(RESAMPLING);
        this.profile = args.get(CUTTING_PROFILE);
        this.xyz = args.get(XYZ);
        this.out_options = args.get(OPTIONS);
        this.scale = args.get(SCALE);

        gdal.SetConfigOption("GDAL_RASTERIO_RESAMPLING", this.resampling.name().toUpperCase());
        gdal.SetConfigOption("VRT_SHARED_SOURCE", "0");

        this.outDrv = getDriverByName(args.get(TILE_DRIVER));

        this.outputFolder = Files.createDirectories(args.getDestination());
        this.outExt = args.get(TILE_EXT);

        int minZoom = args.get(MIN_ZOOM);
        int maxZoom = args.get(MAX_ZOOM);
        int clampMaxZoom = args.get(CLAMP_MAX_ZOOM);
        checkArg(minZoom >> 31 == maxZoom >> 31, "minZoom and maxZoom must be either both set or both unset");
        checkArg(minZoom <= maxZoom, "minZoom (%d) must not be greater than maxZoom (%d)", minZoom, maxZoom);
        checkArg(!(clampMaxZoom >= 0 && maxZoom >= 0), "maxZoom and clampMaxZoom may not be used together!");

        checkArg((this.inputDataset = gdal.Open(args.getSource().toString(), GA_ReadOnly)) != null, "unable to open input file: \"%s\"", args.getSource());
        checkArg(this.inputDataset.getRasterCount() != 0, "input file \"%s\" has no raster bands", args.getSource());
        System.out.printf("input file: (%dP x %dL - %d bands)\n",
                this.inputDataset.getRasterXSize(), this.inputDataset.getRasterYSize(), this.inputDataset.getRasterCount());
        this.input_bands = this.inputDataset.GetRasterCount();
        this.input_dataType = DataType.byId(this.inputDataset.GetRasterBand(1).GetRasterDataType());
        this.output_dataType = args.get(DATA_TYPE) == DataType.Default ? this.input_dataType : args.get(DATA_TYPE);
        this.allBands = IntStream.rangeClosed(1, this.input_bands).toArray();
        this.tlBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(this.tileSize * this.tileSize * DataType.Float64.sizeBytes() * this.input_bands).order(ByteOrder.nativeOrder()));
        this.input_noDataValues = getNoDataValues(this.inputDataset);
        this.output_noDataValues = args.get(A_NODATA) == null ? this.input_noDataValues : args.get(A_NODATA);
        checkState(this.input_noDataValues.length == this.output_noDataValues.length, "a_nodata must contain %d values, but found %d", this.input_noDataValues.length, this.output_noDataValues.length);

        this.in_srs = setupInputSrs(this.inputDataset, args.get(S_SRS));

        if (this.profile != CuttingProfile.RASTER) {
            checkArg(this.in_srs != null, "Input file has unknown SRS! Use '--s_srs EPSG:<xyz> or similar to provide source reference system.");
            checkArg(hasGeoReference(this.inputDataset), "There is no georeference - neither affine transformation (worldfile)\n"
                                                         + "nor GCPs. You can generate only 'raster' profile tiles.\n"
                                                         + "Either gdal2tiles with parameter -p 'raster' or use another GIS\n"
                                                         + "software for georeference e.g. gdal_transform -gcp / -a_ullr / -a_srs");
        }

        this.out_srs = this.profile.getSrsForInput(this.inputDataset, this.in_srs);
        this.out_srs.SetAxisMappingStrategy(OAMS_TRADITIONAL_GIS_ORDER);
        this.out_srsWkt = this.out_srs.ExportToWkt();

        Dataset warpedInputDataset = this.inputDataset;
        if (this.profile != CuttingProfile.RASTER) {
            if (!this.in_srs.ExportToWkt().equals(this.out_srs.ExportToWkt()) || this.inputDataset.GetGCPCount() != 0) {
                warpedInputDataset = gdal.AutoCreateWarpedVRT(this.inputDataset, this.in_srs.ExportToWkt(), this.out_srs.ExportToWkt(), this.resampling.gdal());
            }
        }
        System.out.printf("projected input file: (%dP x %dL - %d bands)\n",
                warpedInputDataset.getRasterXSize(), warpedInputDataset.getRasterYSize(), warpedInputDataset.getRasterCount());
        String tempFile = Files.createTempFile("tiles", ".vrt").toString();
        getDriverByName("VRT").CreateCopy(tempFile, warpedInputDataset);
        this.tlWarpedInputDataset = new ThreadLocal<>();

        this.forkJoinPool = new ForkJoinPool(ForkJoinPool.getCommonPoolParallelism(), pool -> new ForkJoinWorkerThread(pool) {
            @Override
            protected void onStart() {
                //synchronize on opening the input dataset, since having multiple threads open the same dataset at once seems to cause
                // some kind of lock contention in internal gdal code and can result in multiple minutes of idle time before any work actually gets done
                synchronized (Gdal2Tiles.this.tlWarpedInputDataset) {
                    Gdal2Tiles.this.tlWarpedInputDataset.set(gdal.Open(tempFile, GA_ReadOnly));
                }

                super.onStart();
            }

            @Override
            protected void onTermination(Throwable exception) {
                Gdal2Tiles.this.tlWarpedInputDataset.get().delete();
                Gdal2Tiles.this.tlWarpedInputDataset.remove();

                super.onTermination(exception);
            }
        }, null, false);

        this.out_gt = warpedInputDataset.GetGeoTransform();
        this.out_rasterXSize = warpedInputDataset.getRasterXSize();
        this.out_rasterYSize = warpedInputDataset.getRasterYSize();
        this.out_bounds = new Bounds2d(
                this.out_gt[0],
                this.out_gt[0] + warpedInputDataset.GetRasterXSize() * this.out_gt[1],
                this.out_gt[3] - warpedInputDataset.GetRasterYSize() * this.out_gt[1],
                this.out_gt[3]);

        CuttingProfile.ProjectionData projectionData = this.profile.getProjectionData(this.tileSize, this.in_srs, this.out_srs, this.inputDataset, warpedInputDataset, this.out_bounds);
        if (minZoom < 0) {
            minZoom = 0;
            maxZoom = projectionData.defaultMaxZoom;
            if (clampMaxZoom >= 0)
                maxZoom = Math.min(maxZoom, clampMaxZoom);
        }
        this.tileMatrix = projectionData.tileMatrix;
        this.tMinMax = projectionData.tMinMax;

        if (warpedInputDataset != this.inputDataset) {
            warpedInputDataset.delete();
        }

        this.minZoom = minZoom;
        this.maxZoom = maxZoom;

        this.emptyTileBuffer = ByteBuffer.allocateDirect(this.tlBuffer.get().capacity()).order(ByteOrder.nativeOrder());
        Dataset ds = this.createMemTile(this.tileSize);
        ds.ReadRaster_Direct(0, 0, this.tileSize, this.tileSize, this.tileSize, this.tileSize, DataType.Float64.gdal(), this.emptyTileBuffer, this.allBands);
        ds.delete();

        this.run();
    }

    protected Dataset createMemTile(int size) {
        Dataset ds = MEM_DRIVER.Create("", size, size, this.input_bands, this.output_dataType.gdal());

        for (int b = 1; b <= this.input_bands; b++) {
            Band band = ds.GetRasterBand(b);
            band.SetScale(1.0d / this.scale);

            Double noData = this.output_noDataValues[b - 1];
            if (noData != null) {
                noData = this.output_dataType.processNodata(noData * this.scale);
                band.SetNoDataValue(noData);
                band.Fill(noData);
            } else {
                band.DeleteNoDataValue();
            }
        }

        return ds;
    }

    protected void scaleValues(@NonNull ByteBuffer buffer) {
        for (int i = 0; i < buffer.capacity(); i += Double.BYTES) {
            buffer.putDouble(i, this.output_dataType.processNodata(buffer.getDouble(i) * this.scale));
        }
    }

    protected boolean isValid(@NonNull TilePos pos) {
        Bounds2l tBounds = this.tMinMax[pos.tz];
        return pos.tx >= tBounds.minX() && pos.tx <= tBounds.maxX() && pos.ty >= tBounds.minY() && pos.ty <= tBounds.maxY();
    }

    protected long getTilePositionCount(int zoom) {
        Bounds2l tBounds = this.tMinMax[zoom];
        return multiplyExact(tBounds.maxX() - tBounds.minX() + 1L, tBounds.maxY() - tBounds.minY() + 1L);
    }

    protected Stream<TilePos> getTilePositions(int zoom) {
        Bounds2l tBounds = this.tMinMax[zoom];
        return LongStream.rangeClosed(tBounds.minY(), tBounds.maxY()).boxed()
                .flatMap(_ty -> {
                    long ty = _ty; //unbox lol
                    return LongStream.rangeClosed(tBounds.minX(), tBounds.maxX())
                            .mapToObj(tx -> new TilePos(tx, ty, zoom));
                });
    }

    protected TileDetail getTileDetail(@NonNull TilePos pos) {
        Path tileFile = this.tileFile(pos.tz, pos.tx, pos.ty);
        Bounds2d b = this.tileMatrix.tileBounds(new Point2l(pos.tx, pos.ty), pos.tz);

        GeoQuery q;
        if (this.profile != CuttingProfile.RASTER) {
            q = geoQuery(this.out_gt, this.out_rasterXSize, this.out_rasterYSize, this.tileSize, b.minX(), b.maxY(), b.maxX(), b.minY());
        } else {
            int x = (int) pos.tx * this.tileSize;
            int y = (int) pos.ty * this.tileSize;
            if (x >= this.out_rasterXSize || y >= this.out_rasterYSize) {
                return null;
            }
            int sizeX = min(this.tileSize, this.out_rasterXSize - x);
            int sizeY = min(this.tileSize, this.out_rasterYSize - y);
            q = new GeoQuery(x, y, sizeX, sizeY, 0, 0, sizeX, sizeY);
        }
        return new TileDetail(pos.tx, pos.ty, pos.tz, q.rx, q.ry, q.rxSize, q.rySize, q.wx, q.wy, q.wxSize, q.wySize, tileFile);
    }

    protected boolean isInputEmpty(@NonNull ByteBuffer buffer) {
        checkState(buffer.capacity() == this.emptyTileBuffer.capacity());

        for (int i = 0; i < buffer.capacity(); i += Double.BYTES) {
            if (buffer.getDouble(i) != this.emptyTileBuffer.getDouble(i)) {
                return false;
            }
        }
        return true;
    }

    @SneakyThrows(IOException.class)
    protected Dataset createBaseTile(@NonNull TileDetail t) {
        Dataset tile = null;
        Dataset input = this.tlWarpedInputDataset.get();

        ByteBuffer buffer = this.tlBuffer.get();

        if (t.rxSize != 0 && t.rySize != 0 && t.wxSize != 0 && t.wySize != 0) {
            PUnsafe.copyMemory(PUnsafe.pork_directBufferAddress(this.emptyTileBuffer), PUnsafe.pork_directBufferAddress(buffer), buffer.capacity());
            int r = input.ReadRaster_Direct(t.rx, t.ry, t.rxSize, t.rySize, t.wxSize, t.wySize, DataType.Float64.gdal(), buffer, this.allBands);

            this.scaleValues(buffer);
            if (r == CE_None && !this.isInputEmpty(buffer)) {
                tile = this.createMemTile(this.tileSize);
                tile.WriteRaster_Direct(t.wx, t.wy, t.wxSize, t.wySize, t.wxSize, t.wySize, DataType.Float64.gdal(), buffer, this.allBands);
            }
        }

        if (tile != null) {
            tile.SetGeoTransform(this.tileMatrix.tileGeotransform(new Point2l(t.tx, t.ty), t.tz));

            Files.createDirectories(t.tileFile.getParent());
            Dataset dataset = this.outDrv.CreateCopy(t.tileFile.toString(), tile, 0, this.out_options);
            dataset.SetSpatialRef(this.out_srs);
            dataset.delete();
        }

        return tile;
    }

    @SneakyThrows(IOException.class)
    protected Dataset createOverviewTile(@NonNull TilePos pos, @NonNull Dataset[] srcs) {
        Path tileFile = this.tileFile(pos.tz, pos.tx, pos.ty);

        Dataset query = this.createMemTile(this.tileSize << 1);
        Dataset tile = this.createMemTile(this.tileSize);
        ByteBuffer buffer = this.tlBuffer.get();

        int cnt = 0, i = 0;
        for (long x = pos.tx * 2L; x < pos.tx * 2L + 2L; x++) {
            for (long y = pos.ty * 2L; y < pos.ty * 2L + 2L; y++, i++) {
                Dataset base = srcs[i];
                if (base == null) {
                    continue;
                }

                int tilePosX = toInt((x - pos.tx * 2L) * this.tileSize);
                int tilePosY = toInt((y - pos.ty * 2L) * this.tileSize);
                if (this.xyz) {
                    tilePosY = this.tileSize - tilePosY;
                }

                int r = base.ReadRaster_Direct(0, 0, this.tileSize, this.tileSize, this.tileSize, this.tileSize, this.output_dataType.gdal(), buffer, this.allBands);
                if (r == CE_None) {
                    query.WriteRaster_Direct(tilePosX, tilePosY, this.tileSize, this.tileSize, this.tileSize, this.tileSize, this.output_dataType.gdal(), buffer, this.allBands);
                }

                cnt++;
            }
        }

        if (cnt != 0) {
            for (int band = 1; band <= this.input_bands; band++) {
                checkState(gdal.RegenerateOverview(query.GetRasterBand(band), tile.GetRasterBand(band), "AVERAGE") == CE_None);
            }

            tile.SetGeoTransform(this.tileMatrix.tileGeotransform(new Point2l(pos.tx, pos.ty), pos.tz));

            Files.createDirectories(tileFile.getParent());
            Dataset dataset = this.outDrv.CreateCopy(tileFile.toString(), tile, 0, this.out_options);
            dataset.SetSpatialRef(this.out_srs);
            dataset.delete();
        } else {
            tile.delete();
            tile = null;
        }

        query.delete();
        return tile;
    }

    protected ForkJoinTask<Dataset> createTile(@NonNull TilePos pos, @NonNull Runnable callback) {
        return Gdal2Tiles.this.isValid(pos)
                ? new RecursiveTask<Dataset>() {
            @Override
            protected Dataset compute() {
                Dataset dataset;
                if (pos.tz == Gdal2Tiles.this.maxZoom) { //max zoom level - generate the tile from the source dataset
                    dataset = Gdal2Tiles.this.createBaseTile(Gdal2Tiles.this.getTileDetail(pos));
                } else { //generate the tile by scaling the 4 children
                    Dataset[] children = ForkJoinTask.invokeAll(Stream.of(pos.below())
                            .map(childPos -> Gdal2Tiles.this.createTile(childPos, callback))
                            .collect(Collectors.toList()))
                            .stream().map(ForkJoinTask::join).toArray(Dataset[]::new);

                    dataset = Gdal2Tiles.this.createOverviewTile(pos, children);

                    Stream.of(children).filter(Objects::nonNull).forEach(Dataset::delete);
                }

                callback.run();
                return dataset;
            }
        }
                : new RecursiveTask<Dataset>() {
            {
                this.complete(null);
            }

            @Override
            protected Dataset compute() {
                return null;
            }
        };
    }

    public void run() {
        long count = IntStream.rangeClosed(this.minZoom, this.maxZoom).mapToLong(this::getTilePositionCount).sum();
        System.out.printf("rendering %d tiles...\n", count);
        ProgressMonitor monitor = new ProgressMonitor(count);

        this.getTilePositions(this.minZoom)
                .map(pos -> this.createTile(pos, monitor::step))
                .peek(this.forkJoinPool::submit)
                .collect(Collectors.toList()).stream() //buffer into a list to force all tasks to be submitted before continuing
                .map(ForkJoinTask::join)
                .filter(Objects::nonNull)
                .forEach(Dataset::delete);

        System.out.println("done!");
    }

    protected Path tileFile(int zoom, long tx, long ty) {
        if (this.xyz) { //TODO: this needs to be flipped twice if xyz AND raster
            ty = ((1L << zoom) - 1L) - ty;
        }
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
        WGS84 {
            @Override
            public SpatialReference getSrsForInput(@NonNull Dataset inputDataset, SpatialReference s_srs) {
                SpatialReference t_srs = new SpatialReference();
                t_srs.ImportFromEPSG(4326);
                return t_srs;
            }

            @Override
            public ProjectionData getProjectionData(int tileSize, @NonNull SpatialReference s_srs, @NonNull SpatialReference t_srs, @NonNull Dataset inputDataset, @NonNull Dataset warpedInputDataset, @NonNull Bounds2d out_bounds) {
                ProjectionData data = new ProjectionData();
                data.tileMatrix = new WGS84TileMatrix(t_srs, tileSize);
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
                return s_srs.Clone();
            }

            @Override
            public ProjectionData getProjectionData(int tileSize, @NonNull SpatialReference s_srs, @NonNull SpatialReference t_srs, @NonNull Dataset inputDataset, @NonNull Dataset warpedInputDataset, @NonNull Bounds2d out_bounds) {
                ProjectionData data = new ProjectionData();

                double log2 = log10(2.0d);
                int nativeZoom = max(0, ceilI(max(
                        log10(warpedInputDataset.GetRasterXSize() / (double) tileSize) / log2,
                        log10(warpedInputDataset.GetRasterYSize() / (double) tileSize) / log2)));
                data.defaultMaxZoom = nativeZoom;

                data.tileMatrix = new RasterTileMatrix(tileSize, tileSize << nativeZoom, warpedInputDataset.GetGeoTransform());
                data.tMinMax = IntStream.range(0, TileMatrix.MAX_ZOOM_LEVEL)
                        .mapToObj(zoom -> {
                            Point2l tmin = data.tileMatrix.metersToTile(new Point2d(0.0d, 0.0d), zoom);
                            Point2l tmax = data.tileMatrix.metersToTile(new Point2d(warpedInputDataset.GetRasterXSize(), warpedInputDataset.GetRasterYSize()), zoom);
                            //return new Bounds2l(max(0L, tmin.x()), min((1L << zoom) - 1L, tmax.x()), max(0L, tmin.y()), min((1L << zoom) - 1L, tmax.y()));
                            return new Bounds2l(min(tmin.x(), tmax.x()), max(tmin.x(), tmax.x()), min(tmin.y(), tmax.y()), max(tmin.y(), tmax.y()));
                        })
                        .toArray(Bounds2l[]::new);

                return data;
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

    @RequiredArgsConstructor
    protected static class TilePos {
        public final long tx;
        public final long ty;
        public final int tz;

        public TilePos[] below() {
            return new TilePos[]{
                    new TilePos((this.tx << 1L) + 0L, (this.ty << 1L) + 0L, this.tz + 1),
                    new TilePos((this.tx << 1L) + 0L, (this.ty << 1L) + 1L, this.tz + 1),
                    new TilePos((this.tx << 1L) + 1L, (this.ty << 1L) + 0L, this.tz + 1),
                    new TilePos((this.tx << 1L) + 1L, (this.ty << 1L) + 1L, this.tz + 1)
            };
        }
    }
}
