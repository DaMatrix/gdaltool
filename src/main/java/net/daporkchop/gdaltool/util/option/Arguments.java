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

package net.daporkchop.gdaltool.util.option;

import lombok.NonNull;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public class Arguments {
    protected static final Object NULL_VALUE = new Object[0];

    protected final Map<String, Option> options;
    protected final Map<Option, Object> values = new IdentityHashMap<>();
    protected final AtomicBoolean loaded = new AtomicBoolean(false);
    protected final boolean hasSrc;
    protected final boolean hasDst;

    public Arguments(boolean hasSrc, boolean hasDst, @NonNull Option... options) {
        this.options = Arrays.stream(options).collect(Collectors.toMap(o -> String.format("-%s", o.name()), o -> o));

        this.hasSrc = hasSrc;
        this.hasDst = hasDst;
    }

    public void load(@NonNull Iterator<String> itr) {
        if (this.loaded.compareAndSet(false, true)) {
            boolean foundPaths = false;
            boolean foundSrc = !this.hasSrc;
            boolean foundDst = false;
            while (itr.hasNext()) {
                String arg = itr.next();
                if (arg.charAt(0) == '-') {
                    if (foundPaths) {
                        throw new IllegalStateException("Argument after paths!");
                    } else {
                        Option option = this.options.get(arg);
                        if (option == null) {
                            throw new IllegalArgumentException(String.format("Unknown argument: \"%s\"!", arg));
                        } else if (this.values.containsKey(option)) {
                            throw new IllegalStateException(String.format("Argument \"%s\" already present!", arg));
                        } else {
                            Object value = option.parse(arg, itr);
                            this.values.put(option, value == null ? NULL_VALUE : value);
                        }
                    }
                } else {
                    foundPaths = true;
                    if (foundDst) {
                        throw new IllegalStateException("both source and destination were already found!");
                    } else if (foundSrc) {
                        checkArg(this.hasDst, "no destination expected!");
                        foundDst = true;
                        this.values.put(Option.DESTINATION, Option.DESTINATION.parse(arg, itr));
                    } else {
                        foundSrc = true;
                        this.values.put(Option.SOURCE, Option.SOURCE.parse(arg, itr));
                    }
                }
            }
            if (this.hasDst && !foundDst) {
                throw new IllegalArgumentException("Destination not found!");
            } else if (this.hasSrc && !foundSrc) {
                throw new IllegalArgumentException("Source not found!");
            }
            this.options.forEach((name, option) -> {
                if (!this.values.containsKey(option)) {
                    this.values.put(option, NULL_VALUE);
                }
            });
        }
    }

    public Path getDestination() {
        return this.get(Option.DESTINATION);
    }

    public Path getSource() {
        return this.get(Option.SOURCE);
    }

    public <V> V get(@NonNull Option<V> option) {
        @SuppressWarnings("unchecked")
        V val = (V) this.values.get(option);
        if (val == null) {
            throw new IllegalArgumentException(String.format("Unknown option: \"%s\"!", option.name()));
        } else if (val == NULL_VALUE) {
            return option.fallbackValue();
        } else {
            return val;
        }
    }

    public <V> boolean has(@NonNull Option<V> option) {
        return this.values.containsKey(option) && this.values.get(option) != NULL_VALUE;
    }
}
