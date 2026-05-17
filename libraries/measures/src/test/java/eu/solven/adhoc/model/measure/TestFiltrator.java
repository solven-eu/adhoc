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

import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.ISliceFilter;

/**
 * Pins {@link Filtrator#toString()} — used in EXPLAIN logs, plan-tree labels, and exception messages. The format is a
 * stability surface: changing it (e.g. adding always-rendered fields, dropping the conditional-tags skip) breaks the
 * Pivotable Mermaid label rendering and the `[EXPLAIN]` log greppability used by support.
 *
 * @author Benoit Lacelle
 */
public class TestFiltrator {

	@Test
	public void testToString_nominal_omitsEmptyTags() {
		// The conditional-skip on empty tags is the whole point of the hand-rolled toString — Lombok's @ToString
		// would render `tags=[]` and drown the meaningful fields in noise. Pin that behaviour explicitly.
		Filtrator filtrator = Filtrator.builder()
				.name("goal_count")
				.underlying("event_count")
				.filter(ColumnFilter.matchEq("event_code", "G"))
				.build();

		Assertions.assertThat(filtrator)
				.hasToString("Filtrator(name=goal_count, underlying=event_count, filter=event_code==G)");
	}

	@Test
	public void testToString_withTags_rendersTagsBetweenNameAndUnderlying() {
		Filtrator filtrator = Filtrator.builder()
				.name("goal_count")
				.tag("scoring")
				.tag("debug")
				.underlying("event_count")
				.filter(ColumnFilter.matchEq("event_code", "G"))
				.build();

		String s = filtrator.toString();
		Assertions.assertThat(s)
				.startsWith("Filtrator(name=goal_count, tags=[")
				.contains("scoring")
				.contains("debug")
				.contains(", underlying=event_count")
				.contains(", filter=event_code==G")
				.endsWith(")");
	}

	@Test
	public void testToString_matchAllFilter_renders() {
		// Even a no-op MATCH_ALL filter is reported, because the user typically wants to see that a Filtrator was
		// configured with no extra constraint (vs. forgetting to set one) — useful for debugging.
		Filtrator filtrator = Filtrator.builder()
				.name("passthrough")
				.underlying("event_count")
				.filter(ISliceFilter.MATCH_ALL)
				.build();

		Assertions.assertThat(filtrator)
				.hasToString("Filtrator(name=passthrough, underlying=event_count, filter=matchAll)");
	}

	@Test
	public void testToString_fieldOrder_stable() {
		// Pin the exact field order (name → optional tags → underlying → filter). Tooling that greps EXPLAIN logs
		// for `filter=…` relies on `filter` being the last field before the closing parenthesis.
		Filtrator filtrator = Filtrator.builder()
				.name("redcard_count")
				.tag("scoring")
				.underlying("event_count")
				.filter(ColumnFilter.matchIn("event_code", "R", "SY"))
				.build();

		String s = filtrator.toString();
		int idxName = s.indexOf("name=");
		int idxTags = s.indexOf("tags=");
		int idxUnderlying = s.indexOf("underlying=");
		int idxFilter = s.indexOf("filter=");
		Assertions.assertThat(idxName).isGreaterThanOrEqualTo(0);
		Assertions.assertThat(idxTags).isGreaterThan(idxName);
		Assertions.assertThat(idxUnderlying).isGreaterThan(idxTags);
		Assertions.assertThat(idxFilter).isGreaterThan(idxUnderlying);
		Assertions.assertThat(s).endsWith(")");
	}
}
