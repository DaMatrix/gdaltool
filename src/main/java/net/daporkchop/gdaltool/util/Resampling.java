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

package net.daporkchop.gdaltool.util;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;

import java.util.Map;
import java.util.TreeMap;

import static net.daporkchop.lib.common.util.PValidation.*;
import static org.gdal.gdalconst.gdalconst.*;

/**
 * @author DaPorkchop_
 */
@Getter
public enum Resampling {
    NearestNeighbour(GRA_NearestNeighbour, "near"),
    Bilinear(GRA_Bilinear),
    Cubic(GRA_Cubic),
    CubicSpline(GRA_CubicSpline),
    Lanczos(GRA_Lanczos),
    Average(GRA_Average),
    Mode(GRA_Mode),
    Max(GRA_Max),
    Min(GRA_Min),
    Med(GRA_Med),
    Q1(GRA_Q1),
    Q3(GRA_Q3);

    private static final Map<String, Resampling> BY_NAME = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    private static void register(@NonNull String name, @NonNull Resampling resampling) {
        Resampling value = BY_NAME.putIfAbsent(name, resampling);
        checkState(value == null || value == resampling, "duplicate resampling for name \"%s\": %s, %s", name, value, resampling);
    }

    static {
        for (Resampling r : values()) {
            register(r.name(), r);
            for (String alias : r.aliases) {
                register(alias, r);
            }
        }
    }

    public static Resampling byName(@NonNull String name) {
        Resampling resampling = BY_NAME.get(name);
        checkArg(resampling != null, "unknown resampling: \"%s\"!", name);
        return resampling;
    }

    private final int gdal;
    @Getter(AccessLevel.NONE)
    private final String[] aliases;

    Resampling(int gdal, @NonNull String... aliases) {
        this.gdal = gdal;
        this.aliases = aliases;
    }
}
