package eu.solven.adhoc.fsst.v3;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Mode.Throughput)
@Threads(value = 1)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
public class SymbolUtilBenchmark {

	byte[] bytes1 = new byte[] { 1 };
	byte[] bytes2 = new byte[] { 1, 2 };
	byte[] bytes3 = new byte[] { 1, 2, 3 };
	byte[] bytes4 = new byte[] { 1, 2, 3, 5 };
	byte[] bytes5 = new byte[] { 1, 2, 3, 5, 7 };
	byte[] bytes6 = new byte[] { 1, 2, 3, 5, 7, 11 };
	byte[] bytes7 = new byte[] { 1, 2, 3, 5, 7, 11, 13 };
	byte[] bytes8 = new byte[] { 1, 2, 3, 5, 7, 11, 13, 17 };

	@Benchmark
	public long unalignedLoad_1bytes() {
		return SymbolUtil.fsstUnalignedLoad(bytes1, 0);
	}

	@Benchmark
	public long unalignedLoad_2bytes() {
		return SymbolUtil.fsstUnalignedLoad(bytes2, 0);
	}

	@Benchmark
	public long unalignedLoad_3bytes() {
		return SymbolUtil.fsstUnalignedLoad(bytes3, 0);
	}

	@Benchmark
	public long unalignedLoad_4bytes() {
		return SymbolUtil.fsstUnalignedLoad(bytes4, 0);
	}

	@Benchmark
	public long unalignedLoad_5bytes() {
		return SymbolUtil.fsstUnalignedLoad(bytes5, 0);
	}

	@Benchmark
	public long unalignedLoad_6bytes() {
		return SymbolUtil.fsstUnalignedLoad(bytes6, 0);
	}

	@Benchmark
	public long unalignedLoad_7bytes() {
		return SymbolUtil.fsstUnalignedLoad(bytes7, 0);
	}

	@Benchmark
	public long unalignedLoad_8bytes() {
		return SymbolUtil.fsstUnalignedLoad(bytes8, 0);
	}
}
