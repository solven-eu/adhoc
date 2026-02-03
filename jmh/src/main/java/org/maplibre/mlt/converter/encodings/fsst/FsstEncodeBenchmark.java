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

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.springframework.util.Assert;

import eu.solven.adhoc.fsst.v3.ByteSlice;
import eu.solven.adhoc.fsst.v3.FsstAdhoc;

/**
 * Benchmark for FSST encoding methods
 * 
 * @author Benoit Lacelle
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Mode.Throughput)
@Threads(value = 1)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@SuppressWarnings("checkstyle:MagicNumber")
public class FsstEncodeBenchmark {

	private static final FsstAdhoc ADHOC = new FsstAdhoc();

	// ~30 bytes
	private static final byte[] SMALL = FsstTrainingEncodeBenchmark.SMALL;
	private static final eu.solven.adhoc.fsst.v3.SymbolTable TRAINED_SMALL = ADHOC.train(SMALL);
	// ~600 bytes
	private static final byte[] MEDIUM = FsstTrainingEncodeBenchmark.MEDIUM;
	private static final eu.solven.adhoc.fsst.v3.SymbolTable TRAINED_MEDIUM = ADHOC.train(MEDIUM);
	// ~23kb
	private static final byte[] LARGE = FsstTrainingEncodeBenchmark.LARGE;
	private static final eu.solven.adhoc.fsst.v3.SymbolTable TRAINED_LARGE = ADHOC.train(LARGE);
	// 230kb
	private static final byte[] XLARGE;
	private static final eu.solven.adhoc.fsst.v3.SymbolTable TRAINED_XLLARGE;

	static {
		XLARGE = new byte[LARGE.length * 10];
		for (int i = 0; i < 10; i++) {
			System.arraycopy(LARGE, 0, XLARGE, i * LARGE.length, LARGE.length);
		}
		TRAINED_XLLARGE = ADHOC.train(XLARGE);

		Assert.isTrue(SMALL.length == 26, "SMALL length should be 30. Was " + SMALL.length);
		Assert.isTrue(MEDIUM.length == 664, "MEDIUM length should be 30. Was " + MEDIUM.length);
		Assert.isTrue(LARGE.length == 23_038, "LARGE length should be 30. Was " + LARGE.length);
		Assert.isTrue(XLARGE.length == 230_380, "XLARGE length should be 30. Was " + XLARGE.length);
	}

	/**
	 * Enables counting the number of compressed String.
	 * 
	 * TODO This is irrelevant as this is default JMH behavior.
	 */
	@AuxCounters(AuxCounters.Type.EVENTS)
	@State(Scope.Thread)
	public static class EntryCounters {
		public long entry;
	}

	/**
	 * Enables counting the number of compressed bytes.
	 */
	@AuxCounters(AuxCounters.Type.OPERATIONS)
	@State(Scope.Thread)
	public static class ByteCounters {
		public long bytes;
	}

	@Benchmark
	@OperationsPerInvocation(26)
	public ByteSlice encodeSmallAdhoc(EntryCounters ec, ByteCounters bc) {
		ec.entry++;
		bc.bytes += 26;
		return TRAINED_SMALL.encodeAll(SMALL);
	}

	@Benchmark
	@OperationsPerInvocation(664)
	public ByteSlice encodeMediumAdhoc(EntryCounters ec, ByteCounters bc) {
		ec.entry++;
		bc.bytes += 664;
		return TRAINED_MEDIUM.encodeAll(MEDIUM);
	}

	@Benchmark
	@OperationsPerInvocation(23_038)
	public ByteSlice encodeLargeAdhoc(EntryCounters ec, ByteCounters bc) {
		ec.entry++;
		bc.bytes += 23_038;
		return TRAINED_LARGE.encodeAll(LARGE);
	}

	@Benchmark
	@OperationsPerInvocation(230_380)
	public ByteSlice encodeExtraLargeAdhoc(EntryCounters ec, ByteCounters bc) {
		ec.entry++;
		bc.bytes += 230_380;
		return TRAINED_XLLARGE.encodeAll(XLARGE);
	}

}