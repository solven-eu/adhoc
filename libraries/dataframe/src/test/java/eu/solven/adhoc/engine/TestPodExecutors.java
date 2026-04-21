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
package eu.solven.adhoc.engine;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import eu.solven.adhoc.map.factory.IMapBuilderPreKeys;
import eu.solven.adhoc.map.factory.IScopeBinder;
import eu.solven.adhoc.map.factory.ISliceFactory;

public class TestPodExecutors {

	// ── Test fixtures ───────────────────────────────────────────────────────

	/** Minimal {@link ISliceFactory} stub — {@code newMapBuilder} is never exercised by these tests. */
	private static final ISliceFactory PLAIN_FACTORY = keys -> {
		throw new UnsupportedOperationException("not used in PodExecutors tests");
	};

	/**
	 * {@link ISliceFactory} that also implements {@link IScopeBinder}, counting how many times {@code bindScope} was
	 * called so tests can assert that the binding happened.
	 */
	private static final class RecordingBinder implements ISliceFactory, IScopeBinder {
		final AtomicInteger bindScopeInvocations = new AtomicInteger();

		@Override
		public IMapBuilderPreKeys newMapBuilder(Iterable<? extends String> keys) {
			throw new UnsupportedOperationException("not used in PodExecutors tests");
		}

		@Override
		public <R> R bindScope(Callable<R> body) throws Exception {
			bindScopeInvocations.incrementAndGet();
			return body.call();
		}
	}

	/** Test-local {@link IHasExecutorAndSliceFactory} wiring — no pod machinery needed. */
	private static final class TestPod implements IHasExecutorAndSliceFactory {
		private final ISliceFactory sliceFactory;
		private final ListeningExecutorService executor;

		TestPod(ISliceFactory sliceFactory, ListeningExecutorService executor) {
			this.sliceFactory = sliceFactory;
			this.executor = executor;
		}

		@Override
		public ISliceFactory getSliceFactory() {
			return sliceFactory;
		}

		@Override
		public ListeningExecutorService getExecutorService() {
			return executor;
		}
	}

	/** Direct executor: tasks run synchronously on the submitting thread. Lets assertions stay single-threaded. */
	private static TestPod plainPod() {
		return new TestPod(PLAIN_FACTORY, MoreExecutors.newDirectExecutorService());
	}

	private static TestPod bindingPod(RecordingBinder binder) {
		return new TestPod(binder, MoreExecutors.newDirectExecutorService());
	}

	// ── runScoped ───────────────────────────────────────────────────────────

	@Test
	public void testRunScoped_noBinder_runsBodyAndReturnsResult() {
		AtomicBoolean bodyRan = new AtomicBoolean();
		Integer result = PodExecutors.runScoped(plainPod(), () -> {
			bodyRan.set(true);
			return 42;
		});

		Assertions.assertThat(result).isEqualTo(42);
		Assertions.assertThat(bodyRan).isTrue();
	}

	@Test
	public void testRunScoped_withBinder_invokesBindScope() {
		RecordingBinder binder = new RecordingBinder();
		Integer result = PodExecutors.runScoped(bindingPod(binder), () -> 7);

		Assertions.assertThat(result).isEqualTo(7);
		Assertions.assertThat(binder.bindScopeInvocations).hasValue(1);
	}

	@Test
	public void testRunScoped_propagatesRuntimeExceptionUnchanged() {
		IllegalStateException expected = new IllegalStateException("boom");
		Assertions.assertThatThrownBy(() -> PodExecutors.runScoped(plainPod(), () -> {
			throw expected;
		})).isSameAs(expected);
	}

	@Test
	public void testRunScoped_propagatesErrorUnchanged() {
		AssertionError expected = new AssertionError("halt");
		Assertions.assertThatThrownBy(() -> PodExecutors.runScoped(plainPod(), () -> {
			throw expected;
		})).isSameAs(expected);
	}

	@Test
	public void testRunScoped_binderThrowingCheckedException_wrappedAsIllegalState() {
		// A Supplier body can't throw checked exceptions, so the only way to reach runScoped's checked-exception
		// fallback branch is a custom IScopeBinder.bindScope that throws one.
		IOException cause = new IOException("checked");
		TestPod pod = new TestPod(new ThrowingBinder(cause), MoreExecutors.newDirectExecutorService());

		Assertions.assertThatThrownBy(() -> PodExecutors.runScoped(pod, () -> 0))
				.isInstanceOf(IllegalStateException.class)
				.hasCause(cause);
	}

	/** A factory+binder whose {@code bindScope} throws a fixed checked exception — for the wrapping test. */
	private static final class ThrowingBinder implements ISliceFactory, IScopeBinder {
		private final Exception toThrow;

		ThrowingBinder(Exception toThrow) {
			this.toThrow = toThrow;
		}

		@Override
		public IMapBuilderPreKeys newMapBuilder(Iterable<? extends String> keys) {
			throw new UnsupportedOperationException();
		}

		@Override
		public <R> R bindScope(Callable<R> body) throws Exception {
			throw toThrow;
		}
	}

	// ── callScoped ──────────────────────────────────────────────────────────

	@Test
	public void testCallScoped_noBinder_runsBodyAndReturnsResult() throws Exception {
		Integer result = PodExecutors.callScoped(plainPod(), () -> 99);
		Assertions.assertThat(result).isEqualTo(99);
	}

	@Test
	public void testCallScoped_withBinder_invokesBindScope() throws Exception {
		RecordingBinder binder = new RecordingBinder();
		Integer result = PodExecutors.callScoped(bindingPod(binder), () -> 5);

		Assertions.assertThat(result).isEqualTo(5);
		Assertions.assertThat(binder.bindScopeInvocations).hasValue(1);
	}

	@Test
	public void testCallScoped_propagatesCheckedException() {
		IOException expected = new IOException("io");
		Assertions.assertThatThrownBy(() -> PodExecutors.callScoped(plainPod(), () -> {
			throw expected;
		})).isSameAs(expected);
	}

	// ── scopedExecutor ──────────────────────────────────────────────────────

	@Test
	public void testScopedExecutor_runsTaskAndBindsScope() {
		RecordingBinder binder = new RecordingBinder();
		Executor scoped = PodExecutors.scopedExecutor(bindingPod(binder));

		AtomicBoolean ran = new AtomicBoolean();
		scoped.execute(() -> ran.set(true));

		Assertions.assertThat(ran).isTrue();
		Assertions.assertThat(binder.bindScopeInvocations).hasValue(1);
	}

	@Test
	public void testScopedExecutor_noBinder_runsTask() {
		AtomicBoolean ran = new AtomicBoolean();
		PodExecutors.scopedExecutor(plainPod()).execute(() -> ran.set(true));
		Assertions.assertThat(ran).isTrue();
	}

	// ── submitScoped ────────────────────────────────────────────────────────

	@Test
	public void testSubmitScoped_returnsFutureWithResult() throws Exception {
		RecordingBinder binder = new RecordingBinder();
		ListenableFuture<Integer> future = PodExecutors.submitScoped(bindingPod(binder), () -> 123);

		Assertions.assertThat(future.get()).isEqualTo(123);
		Assertions.assertThat(binder.bindScopeInvocations).hasValue(1);
	}

	@Test
	public void testSubmitScoped_noBinder_returnsFutureWithResult() throws Exception {
		ListenableFuture<Integer> future = PodExecutors.submitScoped(plainPod(), () -> 321);
		Assertions.assertThat(future.get()).isEqualTo(321);
	}

	@Test
	public void testSubmitScoped_bodyThrowingCheckedException_surfacesOnFutureGet() {
		IOException cause = new IOException("async-io");
		ListenableFuture<Integer> future = PodExecutors.submitScoped(plainPod(), () -> {
			throw cause;
		});

		Assertions.assertThatThrownBy(future::get)
				.isInstanceOf(java.util.concurrent.ExecutionException.class)
				.hasCause(cause);
	}
}
