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

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import com.google.common.util.concurrent.ListenableFuture;

import eu.solven.adhoc.map.factory.IScopeBinder;
import eu.solven.adhoc.map.factory.ISliceFactory;

/**
 * Helpers to dispatch work through a pod's {@link IHasExecutorAndSliceFactory#getExecutorService() executor} while
 * binding the per-thread scope required by its {@link IHasExecutorAndSliceFactory#getSliceFactory() slice factory}
 * (when the factory implements {@link IScopeBinder}).
 *
 * Call sites should go through this class instead of calling {@link ISliceFactory}'s deprecated {@code ...WithScope}
 * methods directly — this centralises the scope-binding concern and decouples it from {@link ISliceFactory}.
 *
 * @author Benoit Lacelle
 */
public final class PodExecutors {

	// private: utility class with only static helpers.
	private PodExecutors() {
	}

	/**
	 * Runs {@code body} on the caller thread, with the pod's slice-factory scope bound for its duration. A no-op
	 * wrapping for pods whose slice factory does not implement {@link IScopeBinder}.
	 *
	 * @param <R>
	 *            the body's return type
	 * @param pod
	 *            the pod providing the slice factory
	 * @param body
	 *            the body to run
	 * @return the value returned by {@code body}
	 */
	public static <R> R runScoped(IHasExecutorAndSliceFactory pod, Supplier<R> body) {
		try {
			return callScoped(pod, body::get);
		} catch (RuntimeException | Error e) {
			throw e;
		} catch (Exception e) {
			// Unreachable for a Supplier body, but protects against custom IScopeBinder#bindScope overrides that
			// expose checked exceptions.
			throw new IllegalStateException(e);
		}
	}

	/**
	 * Runs {@code body} on the caller thread with the pod's slice-factory scope bound, propagating checked exceptions
	 * thrown by the body.
	 *
	 * @param <R>
	 *            the body's return type
	 * @param pod
	 *            the pod providing the slice factory
	 * @param body
	 *            the body to run
	 * @return the value returned by {@code body}
	 * @throws Exception
	 *             any checked exception propagated from {@code body}
	 */
	@SuppressWarnings("PMD.SignatureDeclareThrowsException")
	public static <R> R callScoped(IHasExecutorAndSliceFactory pod, Callable<R> body) throws Exception {
		ISliceFactory sliceFactory = pod.getSliceFactory();
		if (sliceFactory instanceof IScopeBinder binder) {
			return binder.bindScope(body);
		}
		return body.call();
	}

	/**
	 * Wraps the pod's executor so every task it receives runs with the slice-factory scope bound on the worker thread.
	 *
	 * @param pod
	 *            the pod providing the executor and slice factory
	 * @return a scoped executor
	 */
	public static Executor scopedExecutor(IHasExecutorAndSliceFactory pod) {
		return task -> pod.getExecutorService().execute(() -> runScoped(pod, () -> {
			task.run();
			return null;
		}));
	}

	/**
	 * Submits {@code body} to the pod's executor, binding the slice-factory scope on the worker thread before the body
	 * runs.
	 *
	 * @param <R>
	 *            the body's return type
	 * @param pod
	 *            the pod providing the executor and slice factory
	 * @param body
	 *            the body to run
	 * @return a future for the body's value
	 */
	public static <R> ListenableFuture<R> submitScoped(IHasExecutorAndSliceFactory pod, Callable<R> body) {
		return pod.getExecutorService().submit(() -> callScoped(pod, body));
	}

}
