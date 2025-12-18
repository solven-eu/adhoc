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
package eu.solven.adhoc.map;

import java.util.function.IntUnaryOperator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

public class TestPermutedArrayList {
	@Test
	public void testGetSize() {
		ImmutableList<String> root = ImmutableList.of("a0", "a1", "a2", "a3");
		int[] reorder = { 1, 0, 3, 2 };

		IntUnaryOperator reordering = Mockito.mock(IntUnaryOperator.class);
		Mockito.doAnswer(invok -> {
			return reorder[invok.getArgument(0, Integer.class)];
		}).when(reordering).applyAsInt(Mockito.anyInt());

		PermutedArrayList<String> permuted = PermutedArrayList.<String>builderList(root).reordering(reordering).build();

		Assertions.assertThat(permuted).hasSize(root.size());
		Assertions.assertThat(permuted).element(0).isEqualTo("a1");
		Assertions.assertThat(permuted).element(1).isEqualTo("a0");
		Assertions.assertThat(permuted).element(2).isEqualTo("a3");
		Assertions.assertThat(permuted).element(3).isEqualTo("a2");

	}

	@Disabled("Not relevant since PermutedArrayList consumes a Lambda. Design issue?")
	@Test
	public void testEquals_sameRef() {
		ImmutableList<String> root = ImmutableList.of("a0", "a1", "a2", "a3");
		int[] reorder = { 1, 0, 3, 2 };

		IntUnaryOperator reordering = Mockito.mock(IntUnaryOperator.class);
		Mockito.doAnswer(invok -> {
			return reorder[invok.getArgument(0, Integer.class)];
		}).when(reordering).applyAsInt(Mockito.anyInt());

		PermutedArrayList<String> permuted = PermutedArrayList.<String>builderList(root).reordering(reordering).build();

		PermutedArrayList<String> permuted2 =
				PermutedArrayList.<String>builderList(root).reordering(reordering).build();

		Assertions.assertThat(permuted).isEqualTo(permuted2);
		Mockito.verify(reordering, Mockito.never()).applyAsInt(Mockito.anyInt());
	}

	@Test
	public void testEquals_notPermutation() {
		ImmutableList<String> root = ImmutableList.of("a0", "a1", "a2", "a3");
		int[] reorder = { 1, 0, 3, 2 };

		IntUnaryOperator reordering = Mockito.mock(IntUnaryOperator.class);
		Mockito.doAnswer(invok -> {
			return reorder[invok.getArgument(0, Integer.class)];
		}).when(reordering).applyAsInt(Mockito.anyInt());

		PermutedArrayList<String> permuted = PermutedArrayList.<String>builderList(root).reordering(reordering).build();

		ImmutableList<String> permutedReference = ImmutableList.of("a1", "a0", "a3", "a2");
		Assertions.assertThat(permuted).isEqualTo(permutedReference);
		Mockito.verify(reordering, Mockito.times(root.size())).applyAsInt(Mockito.anyInt());

		Assertions.assertThat(permuted.hashCode()).isEqualTo(permutedReference.hashCode());
	}

	@Test
	public void testEquals_otherPermutation() {
		ImmutableList<String> root = ImmutableList.of("a0", "a1", "a2", "a3");
		int[] reorder = { 1, 0, 3, 2 };

		IntUnaryOperator reordering = Mockito.mock(IntUnaryOperator.class);
		Mockito.doAnswer(invok -> {
			return reorder[invok.getArgument(0, Integer.class)];
		}).when(reordering).applyAsInt(Mockito.anyInt());

		PermutedArrayList<String> permuted = PermutedArrayList.<String>builderList(root).reordering(reordering).build();

		IntUnaryOperator reordering2 = Mockito.mock(IntUnaryOperator.class);
		Mockito.doAnswer(invok -> {
			return reorder[invok.getArgument(0, Integer.class)];
		}).when(reordering2).applyAsInt(Mockito.anyInt());
		PermutedArrayList<String> permuted2 =
				PermutedArrayList.<String>builderList(ImmutableList.<String>of("a0", "a1", "a2", "a3"))
						.reordering(reordering2)
						.build();

		Assertions.assertThat(permuted).isEqualTo(permuted2);
		Mockito.verify(reordering, Mockito.times(root.size())).applyAsInt(Mockito.anyInt());
		Mockito.verify(reordering2, Mockito.times(root.size())).applyAsInt(Mockito.anyInt());
	}

	@Test
	public void testEquals_otherPermutation_differentValue() {
		ImmutableList<String> root = ImmutableList.of("a0", "a1", "a2", "a3");
		int[] reorder = { 1, 0, 3, 2 };

		IntUnaryOperator reordering = Mockito.mock(IntUnaryOperator.class);
		Mockito.doAnswer(invok -> {
			return reorder[invok.getArgument(0, Integer.class)];
		}).when(reordering).applyAsInt(Mockito.anyInt());

		PermutedArrayList<String> permuted = PermutedArrayList.<String>builderList(root).reordering(reordering).build();

		IntUnaryOperator reordering2 = Mockito.mock(IntUnaryOperator.class);
		Mockito.doAnswer(invok -> {
			return reorder[invok.getArgument(0, Integer.class)];
		}).when(reordering2).applyAsInt(Mockito.anyInt());
		PermutedArrayList<String> permuted2 =
				PermutedArrayList.builderList(ImmutableList.of("a0", "a1", "a2", "a3_DIFFERENT"))
						.reordering(reordering2)
						.build();

		Assertions.assertThat(permuted).isNotEqualTo(permuted2);
	}

	@Test
	public void testEquals_otherPermutation_differentOrder() {
		ImmutableList<String> root = ImmutableList.of("a0", "a1", "a2", "a3");
		int[] reorder = { 1, 0, 3, 2 };

		IntUnaryOperator reordering = Mockito.mock(IntUnaryOperator.class);
		Mockito.doAnswer(invok -> {
			return reorder[invok.getArgument(0, Integer.class)];
		}).when(reordering).applyAsInt(Mockito.anyInt());

		PermutedArrayList<String> permuted = PermutedArrayList.<String>builderList(root).reordering(reordering).build();
		int[] reorder2 = { 1, 0, 2, 3 };
		IntUnaryOperator reordering2 = Mockito.mock(IntUnaryOperator.class);
		Mockito.doAnswer(invok -> {
			return reorder2[invok.getArgument(0, Integer.class)];
		}).when(reordering2).applyAsInt(Mockito.anyInt());
		PermutedArrayList<String> permuted2 =
				PermutedArrayList.builderList(ImmutableList.of("a0", "a1", "a2", "a3")).reordering(reordering2).build();

		Assertions.assertThat(permuted).isNotEqualTo(permuted2);
	}
}
