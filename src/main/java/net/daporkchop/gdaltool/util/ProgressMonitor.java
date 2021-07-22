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

import lombok.RequiredArgsConstructor;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
@RequiredArgsConstructor
public class ProgressMonitor {
    protected static final String CONTROL_SEQUENCE = '\u001B' + "[";
    protected static final String RESET_LINE = CONTROL_SEQUENCE + "1G" + CONTROL_SEQUENCE + "2K";

    protected static final boolean ANSI = "YES".equalsIgnoreCase(System.getenv().getOrDefault("ANSI", "YES"));

    protected long done;
    protected final long total;

    protected long lastTime;
    protected long lastDone;
    protected final double[] speeds = IntStream.range(0, 120).mapToDouble(i -> Double.NaN).toArray();

    public synchronized void step() {
        checkState(this.done < this.total, "already done?!?");
        this.done++;

        long now = System.nanoTime();
        if (this.done == this.total || this.lastTime + TimeUnit.SECONDS.toNanos(1L) <= now) {
            System.arraycopy(this.speeds, 0, this.speeds, 1, this.speeds.length - 1);
            this.speeds[0] = ((this.done - this.lastDone) * TimeUnit.SECONDS.toNanos(1L)) / (double) (now - this.lastTime);
            this.lastDone = this.done;
            this.lastTime = now;

            double speed = DoubleStream.of(this.speeds).filter(d -> !Double.isNaN(d)).average().getAsDouble();
            Duration eta = Duration.ofSeconds((long) ((this.total - this.done) / speed));
            System.out.printf(
                    ANSI
                            ? RESET_LINE + "rendered %d/%d tiles (%.2f%%) @ %.2ftiles/s eta %dd %02dh %02dm %02ds"
                            : "rendered %d/%d tiles (%.2f%%) @ %.2ftiles/s eta %dd %02dh %02dm %02ds\n",
                    this.done, this.total,
                    (double) this.done / this.total * 100.0d,
                    speed,
                    eta.toDays(), eta.toHours() - TimeUnit.DAYS.toHours(eta.toDays()), eta.toMinutes() - TimeUnit.HOURS.toMinutes(eta.toHours()), eta.getSeconds() - TimeUnit.MINUTES.toSeconds(eta.toMinutes()));

            if (ANSI) {
                if (this.done == this.total) {
                    System.out.println();
                } else {
                    System.out.flush();
                }
            }
        }
    }
}
