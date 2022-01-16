/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2020-2022 DaPorkchop_
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
import net.daporkchop.gdaltool.Main;
import net.daporkchop.gdaltool.util.geom.Bounds2d;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import org.gdal.osr.SpatialReference;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
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
public class Bounds {
    static {
        Main.init();
    }

    public static void main(@NonNull String[] args) {
        checkArg(args.length == 1, "usage: bounds <input_dataset>");

        new Bounds(Paths.get(args[0]), new Options());
    }

    protected final Dataset inputDataset;

    protected final SpatialReference in_srs;
    protected final SpatialReference out_srs;
    protected final String out_srsWkt;

    protected final double[] out_gt;
    protected final int out_rasterXSize;
    protected final int out_rasterYSize;

    @SneakyThrows(IOException.class)
    public Bounds(@NonNull Path inputFile, @NonNull Options options) {
        checkArg((this.inputDataset = gdal.Open(inputFile.toString(), GA_ReadOnly)) != null, "unable to open input file: \"%s\"", inputFile);
        checkArg(this.inputDataset.getRasterCount() != 0, "input file \"%s\" has no raster bands", inputFile);

        this.in_srs = setupInputSrs(this.inputDataset, options.s_srs());

        checkArg(this.in_srs != null, "Input file has unknown SRS! Use '--s_srs EPSG:<xyz> or similar to provide source reference system.");
        checkArg(hasGeoReference(this.inputDataset), "There is no georeference - neither affine transformation (worldfile)\n"
                                                     + "nor GCPs. You can generate only 'raster' profile tiles.\n"
                                                     + "Either gdal2tiles with parameter -p 'raster' or use another GIS\n"
                                                     + "software for georeference e.g. gdal_transform -gcp / -a_ullr / -a_srs");

        this.out_srs = new SpatialReference();
        this.out_srs.SetAxisMappingStrategy(OAMS_TRADITIONAL_GIS_ORDER);
        this.out_srs.ImportFromEPSGA(4326);
        this.out_srsWkt = this.out_srs.ExportToWkt();

        Dataset warpedInputDataset = this.inputDataset;
        if (!this.in_srs.ExportToWkt().equals(this.out_srs.ExportToWkt()) || this.inputDataset.GetGCPCount() != 0) {
            warpedInputDataset = gdal.AutoCreateWarpedVRT(this.inputDataset, this.in_srs.ExportToWkt(), this.out_srs.ExportToWkt());
        }
        String tempFile = Files.createTempFile("tiles", ".vrt").toString();
        getDriverByName("VRT").CreateCopy(tempFile, warpedInputDataset);

        this.out_gt = warpedInputDataset.GetGeoTransform();
        this.out_rasterXSize = warpedInputDataset.getRasterXSize();
        this.out_rasterYSize = warpedInputDataset.getRasterYSize();
        Bounds2d out_bounds = new Bounds2d(
                this.out_gt[0],
                this.out_gt[0] + warpedInputDataset.GetRasterXSize() * this.out_gt[1],
                this.out_gt[3] - warpedInputDataset.GetRasterYSize() * this.out_gt[1],
                this.out_gt[3]);

        double minX = min(out_bounds.minX(), out_bounds.maxX());
        double maxX = max(out_bounds.minX(), out_bounds.maxX());
        double minZ = min(out_bounds.minY(), out_bounds.maxY());
        double maxZ = max(out_bounds.minY(), out_bounds.maxY());

        if (options.format().isPresent()) {
            System.out.printf(options.format().get().format, minX, maxX, minZ, maxZ);
        } else {
            for (Format format : Format.values()) {
                System.out.printf("%s:\n\n", format.name);
                System.out.printf(format.format, minX, maxX, minZ, maxZ);
                System.out.println();
            }
        }

        warpedInputDataset.delete();
        this.inputDataset.delete();
    }

    @RequiredArgsConstructor
    private enum Format {
        TERRAPLUSPLUS_v1("Terra++ v1.0",
                ""
                + "            \"minX\": %1$s,\n"
                + "            \"maxX\": %2$s,\n"
                + "            \"minZ\": %3$s,\n"
                + "            \"maxZ\": %4$s\n\n"),
        TERRAPLUSPLUS_v1_MIN("Terra++ v1.0 (minified)",
                "\"minX\":%1$s,\"maxX\":%2$s,\"minZ\":%3$s,\"maxZ\":%4$s"),
        TERRAPLUSPLUS_v2("Terra++ v2.0",
                                 ""
                                 + "        \"projection\": \"EPSG:4326\",\n"
                                 + "        \"geometry\": {\n"
                                 + "            \"type\": \"Polygon\",\n"
                                 + "            \"coordinates\": [\n"
                                 + "                [\n"
                                 + "                    [%1$s, %3$s],\n"
                                 + "                    [%2$s, %3$s],\n"
                                 + "                    [%2$s, %4$s],\n"
                                 + "                    [%1$s, %4$s],\n"
                                 + "                    [%1$s, %3$s]\n"
                                 + "                ]\n"
                                 + "            ]\n"
                                 + "        }\n");

        @NonNull
        private final String name;
        @NonNull
        private final String format;
    }

    @Getter
    @Setter
    @ToString
    @EqualsAndHashCode
    public static class Options {
        protected String s_srs = System.getProperty("s_srs");

        protected Optional<Format> format = Optional.ofNullable(System.getProperty("format")).map(Format::valueOf);
    }
}
