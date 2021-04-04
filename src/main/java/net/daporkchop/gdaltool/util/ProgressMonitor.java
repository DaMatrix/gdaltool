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

/**
 * @author DaPorkchop_
 */
public class ProgressMonitor {
    protected static final double STEP = 2.5d;

    protected long done;
    protected double progress;
    protected final long total;

    public ProgressMonitor(long total) {
        this.total = total;

        System.out.print(0);
        System.out.flush();
    }

    public synchronized void step() {
        this.done++;
        double progress = (double) this.done / this.total * 100.0d;
        if (progress >= this.progress + STEP) {
            do {
                this.progress += STEP;
                if ((long) this.progress % 10L == 0) {
                    System.out.print((long) this.progress);
                    if (this.progress == 100.0d) {
                        System.out.println();
                    }
                } else {
                    System.out.print('.');
                }
            }
            while (progress >= this.progress + STEP);
            System.out.flush();
        }
    }
}
