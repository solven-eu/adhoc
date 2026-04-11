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
package eu.solven.adhoc.dataframe.column;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import eu.solven.adhoc.dataframe.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.dataframe.column.navigable.BloomKeyPresencePreScreenFactory;
import eu.solven.adhoc.dataframe.column.navigable.MultitypeNavigableColumn;
import eu.solven.adhoc.dataframe.column.navigable.NoopKeyPresencePreScreenFactory;
import eu.solven.adhoc.dataframe.column.navigable_else_hash.MultitypeNavigableElseHashColumn;

/**
 * Compares insertion performance of {@link IMultitypeColumnFastGet} implementations under two key orderings:
 * <ul>
 * <li>{@code SORTED} — keys are inserted in natural ascending order, exercising the append-last fast path of
 * {@link MultitypeNavigableColumn};</li>
 * <li>{@code RANDOM} — keys are shuffled with a fixed seed, exercising the binary-search slow path (or the bloom
 * pre-screen) of {@link MultitypeNavigableColumn} and the hash-table path of {@link MultitypeHashColumn}.</li>
 * </ul>
 *
 * <p>
 * The benchmark covers four columns:
 * <ul>
 * <li>{@code navigableBloom} — {@link MultitypeNavigableColumn} with the default
 * {@link BloomKeyPresencePreScreenFactory},</li>
 * <li>{@code navigableNoop} — {@link MultitypeNavigableColumn} with {@link NoopKeyPresencePreScreenFactory} (legacy
 * always-fall-through behavior),</li>
 * <li>{@code hash} — {@link MultitypeHashColumn},</li>
 * <li>{@code navigableElseHash} — {@link MultitypeNavigableElseHashColumn} which routes optimal keys to the navigable
 * column and unordered keys to the hash column.</li>
 * </ul>
 *
 * <p>
 * Each measurement {@code @Setup(Level.Invocation)} rebuilds a fresh column so that successive iterations do not
 * accumulate state. The benchmark inserts {@link #size} long values keyed by string in the configured order.
 *
 * @author Benoit Lacelle
 */
@SuppressWarnings({ "checkstyle:MemberName", "checkstyle:MagicNumber", "PMD.AvoidDuplicateLiterals" })
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
public class BenchmarkMultitypeColumnInsertion {

	/** Number of distinct keys inserted per benchmark invocation. */
	@Param({ "1024", "16384" })
	private int size;

	/** Insertion order: {@code SORTED} (ascending) or {@code RANDOM} (shuffled with a fixed seed). */
	@Param({ "SORTED", "RANDOM" })
	private Order order;

	/** Pre-built key list — same instance reused across iterations to keep allocation out of the measurement. */
	private List<String> keys;

	private MultitypeNavigableColumn<String> navigableBloom;
	private MultitypeNavigableColumn<String> navigableNoop;
	private MultitypeHashColumn<String> hash;
	private MultitypeNavigableElseHashColumn<String> navigableElseHash;
	private MultitypeNavigableElseHashColumn<String> navigableElseHashBloom;

	public enum Order {
		SORTED, RANDOM
	}

	@Setup(Level.Trial)
	public void setupKeys() {
		keys = new ArrayList<>(size);
		for (int i = 0; i < size; i++) {
			// Zero-padded so natural string ordering matches numeric ordering.
			keys.add(String.format("k%010d", i));
		}
		if (order == Order.RANDOM) {
			// Fixed seed so every iteration shuffles the same way (reproducibility).
			Collections.shuffle(keys, new SecureRandom(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 }));
		}
	}

	@Setup(Level.Invocation)
	public void resetColumns() {
		navigableBloom = MultitypeNavigableColumn.<String>builder()
				.capacity(size)
				.presenceFilterFactory(BloomKeyPresencePreScreenFactory.INSTANCE)
				.build();
		navigableNoop = MultitypeNavigableColumn.<String>builder()
				.capacity(size)
				.presenceFilterFactory(NoopKeyPresencePreScreenFactory.INSTANCE)
				.build();
		hash = MultitypeHashColumn.<String>builder().capacity(size).build();
		navigableElseHash = MultitypeNavigableElseHashColumn.<String>builder().hash(MultitypeHashColumn.<String>builder().capacity(size).build()).build();
		navigableElseHashBloom =  MultitypeNavigableElseHashColumn.<String>builder()
				.hash(MultitypeHashColumn.<String>builder().capacity(size).build())
				.navigable(MultitypeNavigableColumn.<String>builder()
				.capacity(size)
				.presenceFilterFactory(NoopKeyPresencePreScreenFactory.INSTANCE)
				.build()).build();
	}

	@Benchmark
	public MultitypeNavigableColumn<String> insert_navigableBloom() {
		keys.forEach(k -> navigableBloom.append(k).onLong(1L));
		return navigableBloom;
	}

	@Benchmark
	public MultitypeNavigableColumn<String> insert_navigableNoop() {
		keys.forEach(k -> navigableNoop.append(k).onLong(1L));
		return navigableNoop;
	}

	@Benchmark
	public MultitypeHashColumn<String> insert_hash() {
		keys.forEach(k -> hash.append(k).onLong(1L));
		return hash;
	}

	@Benchmark
	public MultitypeNavigableElseHashColumn<String> insert_navigableElseHash() {
		keys.forEach(k -> navigableElseHash.append(k).onLong(1L));
		return navigableElseHash;
	}

	@Benchmark
	public MultitypeNavigableElseHashColumn<String> insert_navigableElseHashBloom() {
		keys.forEach(k -> navigableElseHashBloom.append(k).onLong(1L));
		return navigableElseHashBloom;
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder().include(BenchmarkMultitypeColumnInsertion.class.getSimpleName())
				.forks(1)

				// https://jmh.morethan.io/
				.resultFormat(ResultFormatType.JSON)
				.result("jmh/target/" + System.currentTimeMillis() + "-multitypeColumnInsertion.json")

				.build();
		new Runner(opt).run();
	}
}
