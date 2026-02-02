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
package eu.solven.adhoc.fsst.v3;

import java.util.concurrent.TimeUnit;

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

/**
 * Benchmarks for SymbolUtil, especially `fsstUnalignedLoad` which is one of FSST hotspots.
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
public class SymbolUtilBenchmark {

	byte[] bytes1 = new byte[] { 1 };
	byte[] bytes2 = new byte[] { 1, 2 };
	byte[] bytes3 = new byte[] { 1, 2, 3 };
	byte[] bytes4 = new byte[] { 1, 2, 3, 5 };
	byte[] bytes5 = new byte[] { 1, 2, 3, 5, 7 };
	byte[] bytes6 = new byte[] { 1, 2, 3, 5, 7, 11 };
	byte[] bytes7 = new byte[] { 1, 2, 3, 5, 7, 11, 13 };
	byte[] bytes8 = new byte[] { 1, 2, 3, 5, 7, 11, 13, 17 };

	@OperationsPerInvocation(1)
	@Benchmark
	public long unalignedLoad_1bytes() {
		return SymbolUtil.fsstUnalignedLoad(bytes1, 0);
	}

	@OperationsPerInvocation(2)
	@Benchmark
	public long unalignedLoad_2bytes() {
		return SymbolUtil.fsstUnalignedLoad(bytes2, 0);
	}

	@OperationsPerInvocation(3)
	@Benchmark
	public long unalignedLoad_3bytes() {
		return SymbolUtil.fsstUnalignedLoad(bytes3, 0);
	}

	@OperationsPerInvocation(4)
	@Benchmark
	public long unalignedLoad_4bytes() {
		return SymbolUtil.fsstUnalignedLoad(bytes4, 0);
	}

	@OperationsPerInvocation(5)
	@Benchmark
	public long unalignedLoad_5bytes() {
		return SymbolUtil.fsstUnalignedLoad(bytes5, 0);
	}

	@OperationsPerInvocation(6)
	@Benchmark
	public long unalignedLoad_6bytes() {
		return SymbolUtil.fsstUnalignedLoad(bytes6, 0);
	}

	@OperationsPerInvocation(7)
	@Benchmark
	public long unalignedLoad_7bytes() {
		return SymbolUtil.fsstUnalignedLoad(bytes7, 0);
	}

	@OperationsPerInvocation(8)
	@Benchmark
	public long unalignedLoad_8bytes() {
		return SymbolUtil.fsstUnalignedLoad(bytes8, 0);
	}
}
