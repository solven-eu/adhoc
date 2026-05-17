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
package eu.solven.adhoc.engine.observability.plan;

import java.util.concurrent.Callable;
import java.util.function.Supplier;

import eu.solven.adhoc.query.AdhocQueryId;
import lombok.experimental.UtilityClass;

/**
 * Thread-scoped binding of the per-query {@link IPlanFragmentSink}. Mirrors the shape of {@code SubmittedQueryIdScope}
 * / {@code CustomMarkerScope} so the family of engine-level scopes stays uniform.
 *
 * <p>
 * The engine binds a real sink for the duration of a query's execution; extensions (calculated columns, combinators,
 * routing measures, …) call {@link #current()} to obtain the sink and publish plan fragments without having to learn
 * the {@code (registry, queryId)} pair themselves.
 *
 * <p>
 * Usage from an extension:
 *
 * <pre>{@code
 * public class MyCalculatedColumn implements ICalculatedColumn { @Override
 * 	public Object computeCoordinate(ISliceWithStep slice) {
 * 		IPlanFragmentSink sink = PlanFragmentScope.current();
 * 		sink.publishLeaf(slice.getStep(), // anchor = the cube step we serve
 * 				new LeafKey("my-calc-column", "value"), // dedup key
 * 				NodeOperator.OTHER,
 * 				"my-calc-column",
 * 				Map.of("language", "sql", "sql", "SELECT case when …"));
 * 		return computed;
 * 	}
 *
 * 	record LeafKey(String columnName, String tag) {
 * 	}
 * }
 * }</pre>
 *
 * <p>
 * Outside any scope, {@link #current()} returns {@link IPlanFragmentSink#NOOP} — extensions never have to guard against
 * a missing scope.
 *
 * @author Benoit Lacelle
 */
@UtilityClass
public final class PlanFragmentScope {
	private static final ScopedValue<IPlanFragmentSink> CURRENT_SINK = ScopedValue.newInstance();

	/**
	 * Binds {@code sink} for the duration of {@code body}.
	 *
	 * @param <R>
	 *            the body's return type
	 * @param sink
	 *            the sink to bind; passing {@code null} is equivalent to binding {@link IPlanFragmentSink#NOOP}
	 * @param body
	 *            the body to run inside the scope
	 * @return the value returned by {@code body}
	 * @throws Exception
	 *             any checked exception propagated from {@code body}
	 */
	@SuppressWarnings("PMD.SignatureDeclareThrowsException")
	public static <R> R callWith(IPlanFragmentSink sink, Callable<R> body) throws Exception {
		IPlanFragmentSink bound;
		if (sink == null) {
			bound = IPlanFragmentSink.NOOP;
		} else {
			bound = sink;
		}
		return ScopedValue.where(CURRENT_SINK, bound).call(body::call);
	}

	/**
	 * Non-throwing variant of {@link #callWith(IPlanFragmentSink, Callable)}.
	 *
	 * @param <R>
	 *            the body's return type
	 * @param sink
	 *            the sink to bind; may be {@code null}
	 * @param body
	 *            the body to run inside the scope
	 * @return the value returned by {@code body}
	 */
	public static <R> R runWith(IPlanFragmentSink sink, Supplier<R> body) {
		try {
			return callWith(sink, body::get);
		} catch (RuntimeException | Error e) {
			throw e;
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * @return the bound sink for the current scope, or {@link IPlanFragmentSink#NOOP} when no scope is active.
	 */
	public static IPlanFragmentSink current() {
		if (CURRENT_SINK.isBound()) {
			return CURRENT_SINK.get();
		}
		return IPlanFragmentSink.NOOP;
	}

	/**
	 * Build a sink that routes every {@code publish} call through
	 * {@link IQueryPlanRegistry#publishFragment(AdhocQueryId, Object, QueryPlanNode)} for the given {@code queryId}.
	 * Returned sink is safe to share across threads — the registry impl handles the concurrency.
	 *
	 * @param registry
	 *            the registry to publish into; if {@link NoopQueryPlanRegistry#INSTANCE}, the returned sink is
	 *            {@link IPlanFragmentSink#NOOP} for a measurable cheap-path benefit on hot calls
	 * @param queryId
	 *            the query id the sink belongs to
	 * @return a sink wired to publish into {@code registry} under {@code queryId}
	 */
	public static IPlanFragmentSink sinkFor(IQueryPlanRegistry registry, AdhocQueryId queryId) {
		if (registry == NoopQueryPlanRegistry.INSTANCE) {
			return IPlanFragmentSink.NOOP;
		}
		return (anchor, subtree) -> registry.publishFragment(queryId, anchor, subtree);
	}
}
