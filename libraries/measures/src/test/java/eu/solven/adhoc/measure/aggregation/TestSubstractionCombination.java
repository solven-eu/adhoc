/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.measure.aggregation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import eu.solven.adhoc.data.cell.ProxyValueReceiver;
import eu.solven.adhoc.data.row.ISlicedRecord;
import eu.solven.adhoc.data.row.SlicedRecordFromSlices;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.sum.SubstractionCombination;
import eu.solven.adhoc.measure.transformator.ICombinationBinding;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.util.NotYetImplementedException;

public class TestSubstractionCombination {
	ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);
	SubstractionCombination combination = new SubstractionCombination();

	@Test
	public void testKey() {
		Assertions.assertThat(SubstractionCombination.isSubstraction("-")).isTrue();
		Assertions.assertThat(SubstractionCombination.isSubstraction("+")).isFalse();

		Assertions.assertThat(SubstractionCombination.isSubstraction(SubstractionCombination.KEY)).isTrue();
		Assertions.assertThat(SubstractionCombination.isSubstraction("foo")).isFalse();
	}

	@Test
	public void testSimpleCases_default() {
		Assertions.assertThat(combination.combine(slice, Arrays.asList())).isNull();
		Assertions.assertThat(combination.combine(slice, Arrays.asList(123))).isEqualTo(123);
		Assertions.assertThat(combination.combine(slice, Arrays.asList("foo"))).isEqualTo("foo");

		Assertions.assertThat(combination.combine(slice, Arrays.asList(12, 24))).isEqualTo(0L - 12);
		Assertions.assertThat(combination.combine(slice, Arrays.asList(12D, 24D))).isEqualTo(0D - 12);

		Assertions.assertThat(combination.combine(slice, Arrays.asList(null, 234D))).isEqualTo(-234D);
		Assertions.assertThat(combination.combine(slice, Arrays.asList(123, null))).isEqualTo(123);

		Assertions.assertThat(combination.combine(slice, Arrays.asList(null, null))).isEqualTo(null);

		Assertions.assertThatThrownBy(() -> combination.combine(slice, Arrays.asList(123, "Arg")))
				.isInstanceOf(NotYetImplementedException.class);
		Assertions.assertThat(combination.combine(slice, Arrays.asList("Arg", null))).isEqualTo("Arg");

		Assertions.assertThatThrownBy(() -> combination.combine(slice, Arrays.asList(null, "Arg")))
				.isInstanceOf(NotYetImplementedException.class);
	}

	@Test
	public void testSimpleCases_fromSlicedRecord() {
		{
			ProxyValueReceiver proxyReceiver = ProxyValueReceiver.builder().build();
			combination.combine(slice,
					SlicedRecordFromSlices.builder().valueProvider(IValueProvider.setValue(123)).build(),
					proxyReceiver);
			Assertions.assertThat(IValueProvider.getValue(proxyReceiver.asValueProvider())).isEqualTo(123L);
		}

		{
			ProxyValueReceiver proxyReceiver = ProxyValueReceiver.builder().build();
			combination.combine(slice,
					SlicedRecordFromSlices.builder()
							.valueProvider(IValueProvider.setValue(123))
							.valueProvider(IValueProvider.NULL)
							.build(),
					proxyReceiver);
		}

		// Coverage
		{
			List<IValueProvider> valueProviders = new ArrayList<>();
			valueProviders.add(IValueProvider.setValue(123));
			valueProviders.add(IValueProvider.setValue(12.34));
			valueProviders.add(IValueProvider.NULL);

			Collections2.permutations(valueProviders).stream().filter(l -> l.size() == 2).forEach(vps -> {
				ProxyValueReceiver proxyReceiver = ProxyValueReceiver.builder().build();
				combination.combine(slice, SlicedRecordFromSlices.builder().valueProviders(vps).build(), proxyReceiver);
				Object o = IValueProvider.getValue(proxyReceiver.asValueProvider());

				Assertions.assertThat(o).isNotNull();
			});
		}
	}

	@Test
	public void testSimpleCases_fromSlicedRecord_fromBinding() {
		Assertions
				.assertThat(IValueProvider.getValue(combination.combine(combination.bind(1),
						slice,
						SlicedRecordFromSlices.builder().valueProvider(IValueProvider.setValue(123)).build())))
				.isEqualTo(123L);

		Assertions.assertThat(IValueProvider.getValue(combination.combine(combination.bind(2),
				slice,
				SlicedRecordFromSlices.builder()
						.valueProvider(IValueProvider.setValue(123))
						.valueProvider(IValueProvider.NULL)
						.build())))
				.isEqualTo(123L);

		// Coverage
		{
			List<IValueProvider> valueProviders = new ArrayList<>();
			valueProviders.add(IValueProvider.setValue(123));
			valueProviders.add(IValueProvider.setValue(234));
			valueProviders.add(IValueProvider.setValue(12.34));
			valueProviders.add(IValueProvider.setValue(23.45));
			valueProviders.add(IValueProvider.NULL);

			ICombinationBinding binding = combination.bind(2);

			Sets.combinations(ImmutableSet.copyOf(valueProviders), 2).forEach(vps -> {

				Collections2.permutations(vps).forEach(permutation -> {
					ISlicedRecord slicedRecord = SlicedRecordFromSlices.builder().valueProviders(permutation).build();
					IValueProvider vpBinding = combination.combine(binding, slice, slicedRecord);
					Object o = IValueProvider.getValue(vpBinding);

					Assertions.assertThat(o).isEqualTo(combination.combine(slice, slicedRecord.asList()));
					Assertions.assertThat(o).isNotNull();

					ProxyValueReceiver vr = ProxyValueReceiver.builder().build();
					combination.combine(slice, slicedRecord, vr);
					Object oProvider = IValueProvider.getValue(vr.asValueProvider());
					Assertions.assertThat(oProvider).isEqualTo(o);
				});
			});
		}
	}

	@Test
	public void testBinding_NullMinusNull() {
		ICombinationBinding binding = combination.bind(2);

		IValueProvider vp2 = combination.combine(binding,
				slice,
				SlicedRecordFromSlices.builder()
						.valueProviders(List.of(IValueProvider.NULL, IValueProvider.NULL))
						.build());

		Assertions.assertThat(IValueProvider.getValue(vp2)).isNull();
	}
}
