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
package eu.solven.adhoc.measure.lambda;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.filter.ISliceFilter;

public class TestLambdaEditor {

	@Test
	public void testEditFilter_identity() {
		LambdaEditor editor =
				new LambdaEditor(Map.of(LambdaEditor.K_LAMBDA, (LambdaEditor.ILambdaFilterEditor) f -> f));

		Assertions.assertThat(editor.editFilter(ISliceFilter.MATCH_ALL)).isSameAs(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void testEditFilter_replaceWithMatchAll() {
		LambdaEditor editor = new LambdaEditor(
				Map.of(LambdaEditor.K_LAMBDA, (LambdaEditor.ILambdaFilterEditor) f -> ISliceFilter.MATCH_ALL));

		ISliceFilter anyFilter = ISliceFilter.MATCH_NONE;
		Assertions.assertThat(editor.editFilter(anyFilter)).isSameAs(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void testConstruct_missingLambda() {
		Assertions.assertThatThrownBy(() -> new LambdaEditor(Map.of())).isInstanceOf(IllegalArgumentException.class);
	}
}
