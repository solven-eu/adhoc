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
package eu.solven.adhoc.table.transcoder;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAliaserBuilder {

	@Test
	public void testEmpty_yieldsIdentity() {
		ITableAliaser aliaser = AliaserBuilder.create().build();

		// Identity always returns null — callers are expected to fall back to the queried name.
		Assertions.assertThat(aliaser).isInstanceOf(IdentityImplicitAliaser.class);
		Assertions.assertThat(aliaser.underlying("anything")).isNull();
	}

	@Test
	public void testSingleMap_returnedDirectly_noCompositeWrapping() {
		ITableAliaser aliaser = AliaserBuilder.create().withMap(Map.of("alias_k", "real_k")).build();

		// Single source: no CompositeTableAliaser wrapping needed.
		Assertions.assertThat(aliaser).isInstanceOf(MapTableAliaser.class);
		Assertions.assertThat(aliaser.underlying("alias_k")).isEqualTo("real_k");
		Assertions.assertThat(aliaser.underlying("unknown")).isNull();
	}

	@Test
	public void testSingleAliaser_returnedDirectly() {
		MapTableAliaser source = MapTableAliaser.builder().aliasToOriginal("a", "b").build();

		ITableAliaser aliaser = AliaserBuilder.create().withAliaser(source).build();

		Assertions.assertThat(aliaser).isSameAs(source);
	}

	@Test
	public void testTwoSources_firstSourceWins() {
		// Source 1 owns the alias, source 2 has the same alias mapping to a DIFFERENT underlying — the first
		// wins per the FirstNotNull chain mode.
		ITableAliaser aliaser =
				AliaserBuilder.create().withMap(Map.of("k", "from_first")).withMap(Map.of("k", "from_second")).build();

		Assertions.assertThat(aliaser.underlying("k")).isEqualTo("from_first");
	}

	@Test
	public void testTwoSources_bothConsultedForDifferentKeys() {
		// Each source owns a distinct alias — composite must consult both.
		ITableAliaser aliaser =
				AliaserBuilder.create().withMap(Map.of("a", "real_a")).withMap(Map.of("b", "real_b")).build();

		Assertions.assertThat(aliaser).isInstanceOf(CompositeTableAliaser.class);
		Assertions.assertThat(aliaser.underlying("a")).isEqualTo("real_a");
		Assertions.assertThat(aliaser.underlying("b")).isEqualTo("real_b");
		Assertions.assertThat(aliaser.underlying("missing")).isNull();
	}

	@Test
	public void testNullAliaser_silentlyIgnored() {
		// Null is a convenience for callers whose source may not provide an aliaser at all.
		ITableAliaser aliaser =
				AliaserBuilder.create().withAliaser(null).withMap(Map.of("k", "real_k")).withAliaser(null).build();

		Assertions.assertThat(aliaser).isInstanceOf(MapTableAliaser.class);
		Assertions.assertThat(aliaser.underlying("k")).isEqualTo("real_k");
	}

	@Test
	public void testEmptyMap_silentlyIgnored() {
		// Empty map adds nothing — same semantics as not calling .withMap at all.
		ITableAliaser aliaser = AliaserBuilder.create().withMap(Map.of()).withMap(Map.of("k", "real_k")).build();

		Assertions.assertThat(aliaser).isInstanceOf(MapTableAliaser.class);
		Assertions.assertThat(aliaser.underlying("k")).isEqualTo("real_k");
	}

	@Test
	public void testRecursive_followsChainedMappings() {
		// k → _k → __k. Without recursive(), `underlying("k")` would return `_k`. With recursive(), it follows
		// the chain to `__k`.
		ITableAliaser aliaser = AliaserBuilder.create().withMap(Map.of("k", "_k", "_k", "__k")).recursive().build();

		Assertions.assertThat(aliaser).isInstanceOf(RecursiveAliaser.class);
		Assertions.assertThat(aliaser.underlying("k")).isEqualTo("__k");
		Assertions.assertThat(aliaser.underlying("_k")).isEqualTo("__k");
	}

	@Test
	public void testRecursive_onEmpty_stillIdentityShape() {
		// recursive() on no sources wraps Identity — still returns null on lookup, no chain to follow.
		ITableAliaser aliaser = AliaserBuilder.create().recursive().build();

		Assertions.assertThat(aliaser.underlying("anything")).isNull();
	}

	@Test
	public void testMixedSources_aliaserAndMap_composedTogether() {
		// Real-world shape: a bring-your-own aliaser plus a schema-derived map.
		MapTableAliaser custom = MapTableAliaser.builder().aliasToOriginal("custom_k", "real_custom").build();
		Map<String, String> schemaDerived = Map.of("schema_k", "real_schema");

		ITableAliaser aliaser = AliaserBuilder.create().withAliaser(custom).withMap(schemaDerived).build();

		Assertions.assertThat(aliaser.underlying("custom_k")).isEqualTo("real_custom");
		Assertions.assertThat(aliaser.underlying("schema_k")).isEqualTo("real_schema");
	}
}
