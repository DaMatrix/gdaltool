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
import java.util.stream.Stream;

import static java.lang.Math.*;
import static net.daporkchop.lib.common.util.PValidation.*;
import static org.gdal.gdalconst.gdalconst.*;

/**
 * @author DaPorkchop_
 */
@Getter
public enum DataType {
    Default(GDT_Unknown) {
        @Override
        public double processNodata(double d) {
            throw new UnsupportedOperationException();
        }
    },
    Byte(GDT_Byte) {
        @Override
        public double processNodata(double d) {
            return min(max(floor(d), java.lang.Byte.MIN_VALUE), java.lang.Byte.MAX_VALUE);
        }
    },
    UInt16(GDT_UInt16) {
        @Override
        public double processNodata(double d) {
            return min(max(floor(d), Character.MIN_VALUE), Character.MAX_VALUE);
        }
    },
    Int16(GDT_Int16) {
        @Override
        public double processNodata(double d) {
            return min(max(floor(d), Short.MIN_VALUE), Short.MAX_VALUE);
        }
    },
    UInt32(GDT_UInt32) {
        @Override
        public double processNodata(double d) {
            return min(max(floor(d), 0), Integer.toUnsignedLong(-1));
        }
    },
    Int32(GDT_Int32) {
        @Override
        public double processNodata(double d) {
            return min(max(floor(d), Integer.MIN_VALUE), Integer.MAX_VALUE);
        }
    },
    Float32(GDT_Float32) {
        @Override
        public double processNodata(double d) {
            return (float) d;
        }
    },
    Float64(GDT_Float64) {
        @Override
        public double processNodata(double d) {
            return d;
        }
    };

    private static final Map<String, DataType> BY_NAME = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    static {
        for (DataType r : values()) {
            register(r.name(), r);
            for (String alias : r.aliases) {
                register(alias, r);
            }
        }
    }

    private static void register(@NonNull String name, @NonNull DataType resampling) {
        DataType value = BY_NAME.putIfAbsent(name, resampling);
        checkState(value == null || value == resampling, "duplicate resampling for name \"%s\": %s, %s", name, value, resampling);
    }

    public static DataType byName(@NonNull String name) {
        DataType resampling = BY_NAME.get(name);
        checkArg(resampling != null, "unknown resampling: \"%s\"!", name);
        return resampling;
    }

    public static DataType byId(int id) {
        return Stream.of(values()).filter(type -> type.gdal == id).findFirst().orElseThrow(IllegalStateException::new);
    }

    private final int gdal;
    @Getter(AccessLevel.NONE)
    private final String[] aliases;

    private final int sizeBytes;

    DataType(int gdal, @NonNull String... aliases) {
        this.gdal = gdal;
        this.aliases = aliases;

        this.sizeBytes = org.gdal.gdal.gdal.GetDataTypeSize(this.gdal) / java.lang.Byte.SIZE;
    }

    public abstract double processNodata(double d);
}
