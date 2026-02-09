/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.maplibre.mlt.converter.encodings.fsst;

import java.util.LongSummaryStatistics;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Enable DEBUG for FSST.
 * 
 * @author Benoit Lacelle
 */
@SuppressWarnings({ "PMD", "checkstyle:all" })
class FsstDebug implements Fsst {
	private final Fsst java = new FsstJava();
	private final Fsst jni = new FsstJni();

	private static final AtomicLong jniTime = new AtomicLong();
	private static final AtomicLong javaTime = new AtomicLong();
	private static final AtomicLong jniSize = new AtomicLong();
	private static final AtomicLong javaSize = new AtomicLong();
	private static final AtomicInteger javaSmaller = new AtomicInteger();
	private static final AtomicInteger jniSmaller = new AtomicInteger();
	private static final AtomicBoolean printed = new AtomicBoolean(false);

	static {
		Runtime.getRuntime().addShutdownHook(new Thread(FsstDebug::printStats));
	}

	@Override
	public SymbolTable encode(byte[] data) {
		new LongSummaryStatistics();

		long a = System.currentTimeMillis();
		var fromJni = jni.encode(data);
		long b = System.currentTimeMillis();
		var fromJava = java.encode(data);
		long c = System.currentTimeMillis();
		jniTime.addAndGet(b - a);
		javaTime.addAndGet(c - b);
		jniSize.addAndGet(fromJni.weight());
		javaSize.addAndGet(fromJava.weight());
		(fromJava.weight() <= fromJni.weight() ? javaSmaller : jniSmaller).incrementAndGet();
		return fromJava;
	}

	public static void printStats() {
		if (!printed.getAndSet(true)) {
			System.err.println("java/jni:" + javaTime
					+ "ms/"
					+ jniTime
					+ "ms "
					+ javaSize
					+ "/"
					+ jniSize
					+ " "
					+ (javaSize.get() * 1d / jniSize.get())
					+ " jni smaller "
					+ jniSmaller
					+ "/"
					+ (javaSmaller.get() + jniSmaller.get()));
		}
	}
}
