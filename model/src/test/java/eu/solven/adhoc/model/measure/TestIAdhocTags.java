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
package eu.solven.adhoc.model.measure;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Pins the literal value of each baked-in tag.
 *
 * <p>
 * These strings are a cross-process contract, not an implementation detail: they travel to clients as plain tags on the
 * cube description, and Pivotable's {@code adhoc-baked-in-tags.js} matches on the same literals. Renaming a constant is
 * free; changing its value silently stops every client acting on that tag, with nothing failing to compile on either
 * side. Hence pinning the values rather than the identifiers.
 * </p>
 *
 * @author Benoit Lacelle
 */
public class TestIAdhocTags {

	@Test
	public void testTagsDrivingClientBehavior() {
		Assertions.assertThat(IAdhocTags.TAG_ESSENTIAL).isEqualTo("essential");
		Assertions.assertThat(IAdhocTags.TAG_HIDDEN).isEqualTo("hidden");
	}

	@Test
	public void testTagsStampedByTheEngine() {
		Assertions.assertThat(IAdhocTags.TAG_CALCULATED).isEqualTo("calculated");
		Assertions.assertThat(IAdhocTags.TAG_GENERATED).isEqualTo("generated");
		Assertions.assertThat(IAdhocTags.TAG_META).isEqualTo("meta");
		Assertions.assertThat(IAdhocTags.TAG_COMPOSITE_FULL).isEqualTo("composite-full");
		Assertions.assertThat(IAdhocTags.TAG_TECHNICAL).isEqualTo("technical");
		Assertions.assertThat(IAdhocTags.TAG_DEBUG).isEqualTo("debug");
	}

	/**
	 * {@link IHasTags} carried the debug tag before {@link IAdhocTags} collected them all; it now delegates, so the two
	 * cannot drift apart.
	 */
	@Test
	public void testHasTagsDelegatesDebug() {
		Assertions.assertThat(IHasTags.TAG_DEBUG).isEqualTo(IAdhocTags.TAG_DEBUG);
	}
}
