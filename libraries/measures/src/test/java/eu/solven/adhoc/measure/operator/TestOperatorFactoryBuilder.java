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

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.filter.editor.IFilterEditor;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.adhoc.measure.sum.SumAggregation;

public class TestOperatorFactoryBuilder {

	/** Probe factory: returns canned values for a single key, throws for everything else. */
	private static final class ProbeOperatorFactory implements IOperatorFactory {
		private final String key;
		private final IAggregation aggregation;

		ProbeOperatorFactory(String key, IAggregation aggregation) {
			this.key = key;
			this.aggregation = aggregation;
		}

		@Override
		public IAggregation makeAggregation(String k, Map<String, ?> options) {
			if (this.key.equals(k)) {
				return aggregation;
			}
			throw new IllegalArgumentException("probe doesn't recognise " + k);
		}

		@Override
		public ICombination makeCombination(String k, Map<String, ?> options) {
			throw new IllegalArgumentException("probe doesn't supply combinations");
		}

		@Override
		public IDecomposition makeDecomposition(String k, Map<String, ?> options) {
			throw new IllegalArgumentException("probe doesn't supply decompositions");
		}

		@Override
		public IFilterEditor makeEditor(String k, Map<String, ?> options) {
			throw new IllegalArgumentException("probe doesn't supply filter editors");
		}

		@Override
		public IOperatorFactory withRoot(IOperatorFactory rootOperatorFactory) {
			return this;
		}
	}

	@Test
	public void testEmpty_yieldsStandard() {
		IOperatorFactory factory = OperatorFactoryBuilder.create().build();

		// Standard set is the no-config default — every project needs the built-ins.
		Assertions.assertThat(factory).isInstanceOf(StandardOperatorFactory.class);
		// Sanity: the standard SUM aggregation is reachable.
		Assertions.assertThat(factory.makeAggregation(SumAggregation.KEY)).isNotNull();
	}

	@Test
	public void testSingleFactory_returnedDirectly_noCompositeWrapping() {
		IAggregation marker = Mocking.markerAggregation();
		IOperatorFactory probe = new ProbeOperatorFactory("CUSTOM_KEY", marker);

		IOperatorFactory factory = OperatorFactoryBuilder.create().withFactory(probe).build();

		Assertions.assertThat(factory).isSameAs(probe);
		Assertions.assertThat(factory.makeAggregation("CUSTOM_KEY")).isSameAs(marker);
	}

	@Test
	public void testCustomThenStandard_customOverridesStandardKey() {
		// Custom defines a SumAggregation.KEY → marker. Even though the standard set also recognises that key,
		// the custom factory comes first in declaration order so its result wins.
		IAggregation marker = Mocking.markerAggregation();
		IOperatorFactory probe = new ProbeOperatorFactory(SumAggregation.KEY, marker);

		IOperatorFactory factory = OperatorFactoryBuilder.create().withFactory(probe).withStandard().build();

		Assertions.assertThat(factory).isInstanceOf(CompositeOperatorFactory.class);
		Assertions.assertThat(factory.makeAggregation(SumAggregation.KEY)).isSameAs(marker);
	}

	@Test
	public void testCustomThenStandard_unknownKeyFallsBackToStandard() {
		// Custom only knows CUSTOM_KEY; for SumAggregation.KEY the composite falls through to the standard set.
		IOperatorFactory probe = new ProbeOperatorFactory("CUSTOM_KEY", Mocking.markerAggregation());

		IOperatorFactory factory = OperatorFactoryBuilder.create().withFactory(probe).withStandard().build();

		// The standard set's SUM is not the marker — composite resolved through the second source.
		Assertions.assertThat(factory.makeAggregation(SumAggregation.KEY))
				.isNotNull()
				.isNotSameAs(Mocking.markerAggregation());
	}

	@Test
	public void testStandardThenCustom_standardWinsOnSharedKey() {
		// Reverse order: standard first, custom second. Standard's SUM resolves first, custom's marker is never
		// reached for SumAggregation.KEY.
		IAggregation marker = Mocking.markerAggregation();
		IOperatorFactory probe = new ProbeOperatorFactory(SumAggregation.KEY, marker);

		IOperatorFactory factory = OperatorFactoryBuilder.create().withStandard().withFactory(probe).build();

		Assertions.assertThat(factory.makeAggregation(SumAggregation.KEY)).isNotSameAs(marker);
	}

	@Test
	public void testNullFactory_silentlyIgnored() {
		IOperatorFactory factory =
				OperatorFactoryBuilder.create().withFactory(null).withStandard().withFactory(null).build();

		// Only the standard registered.
		Assertions.assertThat(factory).isInstanceOf(StandardOperatorFactory.class);
	}

	/** Marker aggregation for identity comparisons in the assertions above. */
	private static final class Mocking {
		// Memoise so multiple calls return the SAME instance — assertions can use isSameAs.
		private static final IAggregation MARKER = (left, right) -> {
			throw new UnsupportedOperationException("marker — never invoked");
		};

		static IAggregation markerAggregation() {
			return MARKER;
		}
	}
}
