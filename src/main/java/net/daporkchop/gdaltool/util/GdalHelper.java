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

package net.daporkchop.gdaltool.util;

import lombok.NonNull;
import lombok.experimental.UtilityClass;
import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.Driver;
import org.gdal.gdal.gdal;
import org.gdal.osr.SpatialReference;

import java.util.Arrays;
import java.util.stream.IntStream;

import static net.daporkchop.lib.common.util.PValidation.*;
import static org.gdal.osr.osr.*;

/**
 * @author DaPorkchop_
 */
@UtilityClass
public class GdalHelper {
    public final Driver MEM_DRIVER = getDriverByName("MEM");

    public Driver getDriverByName(@NonNull String name) {
        Driver driver = gdal.GetDriverByName(name);
        checkArg(driver != null, "unknown driver: \"%s\"", name);
        return driver;
    }

    public Double[] getNoDataValues(@NonNull Dataset dataset) {
        return IntStream.rangeClosed(1, dataset.GetRasterCount())
                .mapToObj(band -> {
                    Double[] tmp = new Double[1];
                    dataset.GetRasterBand(band).GetNoDataValue(tmp);
                    return tmp[0];
                })
                .toArray(Double[]::new);
    }

    public SpatialReference setupInputSrs(@NonNull Dataset dataset, String userSrsOverride) {
        SpatialReference in_srs = null;
        if (userSrsOverride != null) {
            in_srs = new SpatialReference();
            in_srs.SetFromUserInput(userSrsOverride);
        } else {
            String inputSrsWkt = dataset.GetProjection();
            if (inputSrsWkt == null && dataset.GetGCPCount() != 0) {
                inputSrsWkt = dataset.GetGCPProjection();
            }
            if (inputSrsWkt != null && !inputSrsWkt.isEmpty()) {
                in_srs = new SpatialReference();
                in_srs.ImportFromWkt(inputSrsWkt);
            }
        }
        if (in_srs != null) {
            in_srs.SetAxisMappingStrategy(OAMS_TRADITIONAL_GIS_ORDER);
        }
        return in_srs;
    }

    public boolean hasGeoReference(@NonNull Dataset dataset) {
        return !Arrays.equals(dataset.GetGeoTransform(), new double[]{ 0.0d, 1.0d, 0.0d, 0.0d, 0.0d, 1.0d })
               || dataset.GetGCPCount() != 0;
    }
}
