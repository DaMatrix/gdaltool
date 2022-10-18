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

package net.daporkchop.gdaltool;

import net.daporkchop.gdaltool.mode.Bounds;
import net.daporkchop.gdaltool.mode.Gdal2Tiles;
import net.daporkchop.gdaltool.mode.Mode;
import net.daporkchop.gdaltool.util.option.Arguments;
import org.gdal.gdal.gdal;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;

import static net.daporkchop.lib.common.util.PValidation.*;

public class Main {
    private static final Map<String, Supplier<Mode>> MODES = new TreeMap<>();

    static {
        gdal.AllRegister();

        MODES.put("bounds", Bounds::new);
        MODES.put("gdal2tiles", Gdal2Tiles::new);
    }

    /**
     * Dummy method to force class initialization
     */
    public static void init() {
    }

    public static void main(String... args) {
        if (args.length < 1) {
            System.err.println("usage: gdaltool <mode name> [arguments]...");
            System.err.println();
            System.err.println("Available modes:");
            MODES.forEach((name, factory) -> factory.get().printUsage());
            return;
        }

        Supplier<Mode> modeFactory = MODES.get(args[0]);
        if (modeFactory == null) {
            System.err.println("Unknown mode: '" + args[0] + '\'');
            System.err.println();
            System.err.println("Available modes:");
            MODES.forEach((name, factory) -> factory.get().printUsage());
            return;
        }

        Mode mode = modeFactory.get();

        Arguments arguments = mode.arguments();
        arguments.load(Arrays.asList(Arrays.copyOfRange(args, 1, args.length)).iterator());

        mode.run(arguments);
    }
}
