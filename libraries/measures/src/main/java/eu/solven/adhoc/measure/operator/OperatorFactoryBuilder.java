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
package eu.solven.adhoc.measure.operator;

import java.util.ArrayList;
import java.util.List;

/**
 * Composer for building a single {@link IOperatorFactory} from several sources, with deterministic precedence.
 * <p>
 * Most projects compose multiple operator-factory sources: the {@link StandardOperatorFactory} for the built-in
 * {@code SUM} / {@code MIN} / {@code MAX} / etc. operators, plus one or more project-specific factories that supply
 * custom aggregations / combinations / decompositions / filter editors. Composing them by hand means re-discovering
 * {@link CompositeOperatorFactory}'s "first-resolves-wins" semantics every time and remembering to include the standard
 * set.
 *
 * <p>
 * Composition rules:
 * <ul>
 * <li>Sources are tried in declaration order — {@link CompositeOperatorFactory} returns the first source whose
 * {@code make*} call succeeds, mirroring "first-source wins" precedence.</li>
 * <li>{@link #withStandard()} appends a {@link StandardOperatorFactory} at the current position. Call it last when you
 * want your custom factories to override standard keys; call it first to fall back to custom only when the standard set
 * doesn't recognise the key.</li>
 * <li>Empty input → {@link #build()} returns a fresh {@link StandardOperatorFactory} (the most useful no-config default
 * — every project needs the built-ins).</li>
 * <li>Single source → returned directly (no {@link CompositeOperatorFactory} wrapping).</li>
 * <li>Multiple sources → wrapped in {@link CompositeOperatorFactory}.</li>
 * </ul>
 *
 * <p>
 * Typical usage at cube-build time:
 *
 * <pre>{@code
 * IOperatorFactory operators = OperatorFactoryBuilder.create()
 * 		// Project-specific factories first — they override standard keys when both define the same name.
 * 		.withFactory(myProjectOperators)
 * 		// Then fall back to the standard set for everything else.
 * 		.withStandard()
 * 		.build();
 *
 * CubeWrapper.builder()
 * 		.table(table)
 * 		.forest(forest)
 * 		.columnsManager(ColumnsManager.builder().operatorFactory(operators).build())
 * 		.build();
 * }</pre>
 *
 * @author Benoit Lacelle
 */
public final class OperatorFactoryBuilder {

	private final List<IOperatorFactory> sources = new ArrayList<>();

	private OperatorFactoryBuilder() {
	}

	/**
	 * @return a fresh builder with no sources registered. The default {@link #build()} of an empty builder yields a
	 *         {@link StandardOperatorFactory}.
	 */
	public static OperatorFactoryBuilder create() {
		return new OperatorFactoryBuilder();
	}

	/**
	 * Registers an existing {@link IOperatorFactory} as a source. {@code null} entries are silently skipped — useful
	 * when the caller's source might not provide a factory at all (avoids forcing every call site to guard the input).
	 *
	 * @param factory
	 *            the factory to chain into the result, or {@code null} to skip
	 * @return this
	 */
	public OperatorFactoryBuilder withFactory(IOperatorFactory factory) {
		if (factory != null) {
			sources.add(factory);
		}
		return this;
	}

	/**
	 * Appends a fresh {@link StandardOperatorFactory} at the current position. Call it last to use the standard set as
	 * a fallback (custom factories override standard keys); call it first to use custom factories only when the
	 * standard set doesn't recognise the key.
	 *
	 * @return this
	 */
	public OperatorFactoryBuilder withStandard() {
		sources.add(StandardOperatorFactory.builder().build());
		return this;
	}

	/**
	 * @return a single {@link IOperatorFactory} composing every registered source, with first-source-wins precedence.
	 *         Returns a fresh {@link StandardOperatorFactory} when no sources were registered.
	 */
	public IOperatorFactory build() {
		if (sources.isEmpty()) {
			return StandardOperatorFactory.builder().build();
		}
		if (sources.size() == 1) {
			return sources.get(0);
		}
		CompositeOperatorFactory.CompositeOperatorFactoryBuilder composite = CompositeOperatorFactory.builder();
		sources.forEach(composite::operatorFactory);
		return composite.build();
	}
}
