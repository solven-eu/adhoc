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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Composer for building a single {@link ITableAliaser} from several sources, with deterministic precedence.
 * <p>
 * Most projects compose multiple alias sources: an aliaser implied by their SQL schema (e.g. the
 * {@code aliasToOriginal} map produced by {@code JooqTableSupplierBuilder}), one or more user-declared maps for
 * cube-level renames, and possibly third-party aliasers from federation or transcoding layers. Composing them by hand
 * means re-discovering {@link CompositeTableAliaser} / {@link RecursiveAliaser} every time and remembering the right
 * order — easy to forget the schema-side source. This builder makes the inputs explicit and the output a single
 * {@link ITableAliaser} ready to be passed to {@code ColumnsManager.aliaser(...)}.
 *
 * <p>
 * Composition rules:
 * <ul>
 * <li>Sources are tried in declaration order via {@link CompositeTableAliaser.ChainMode#FirstNotNull} — the first
 * non-null lookup wins, mirroring "first-source wins" precedence.</li>
 * <li>Empty input → returns the {@link IdentityImplicitAliaser} (lookup always returns {@code null} so callers fall
 * back to the queried name).</li>
 * <li>Single source → returns that source directly (no composite wrapping).</li>
 * <li>Multiple sources → returns a {@link CompositeTableAliaser}.</li>
 * <li>{@link #recursive()} wraps the final result in {@link RecursiveAliaser}, so chained mappings (e.g. alias A→B,
 * alias B→C resolves A→C) are followed automatically.</li>
 * </ul>
 *
 * <p>
 * Typical usage at cube-build time:
 *
 * <pre>{@code
 * ITableAliaser aliaser = AliaserBuilder.create()
 * 		// Schema-derived: caller-facing aliases declared via JooqJoinBuilder.withAlias(...)
 * 		.withMap(schemaBuilder.getAliasToOriginal())
 * 		// Cube-level renames the user maintains by hand
 * 		.withMap(Map.of("amount_eur", "amount_in_euros"))
 * 		// A bring-your-own aliaser (e.g. PrefixAliaser, federated source)
 * 		.withAliaser(myCustomAliaser)
 * 		.build();
 *
 * CubeWrapper cube = CubeWrapper.builder()
 * 		.table(table)
 * 		.forest(forest)
 * 		.columnsManager(ColumnsManager.builder().aliaser(aliaser).build())
 * 		.build();
 * }</pre>
 *
 * @author Benoit Lacelle
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class AliaserBuilder {

	private final List<ITableAliaser> sources = new ArrayList<>();
	@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
	private boolean recursive;

	/**
	 * @return a fresh builder with no sources registered. The default {@link #build()} of an empty builder yields
	 *         {@link IdentityImplicitAliaser}.
	 */
	public static AliaserBuilder create() {
		return new AliaserBuilder();
	}

	/**
	 * Registers an existing {@link ITableAliaser} as a source. {@code null} entries are silently skipped — useful when
	 * the caller's source might not provide an aliaser at all (avoids forcing every call site to guard the input).
	 *
	 * @param aliaser
	 *            the aliaser to chain into the result, or {@code null} to skip
	 * @return this
	 */
	public AliaserBuilder withAliaser(ITableAliaser aliaser) {
		if (aliaser != null) {
			sources.add(aliaser);
		}
		return this;
	}

	/**
	 * Registers a {@link Map} as a source, wrapped in a {@link MapTableAliaser}. Empty / {@code null} maps are silently
	 * skipped — the same "guard at the source" convenience as {@link #withAliaser(ITableAliaser)}.
	 *
	 * @param aliasToOriginal
	 *            the {@code alias → underlying} map, typically returned by a schema builder's
	 *            {@code getAliasToOriginal()}
	 * @return this
	 */
	public AliaserBuilder withMap(Map<String, String> aliasToOriginal) {
		if (aliasToOriginal != null && !aliasToOriginal.isEmpty()) {
			sources.add(MapTableAliaser.builder().aliasToOriginals(aliasToOriginal).build());
		}
		return this;
	}

	/**
	 * Wraps the final result in {@link RecursiveAliaser}, so chained mappings are followed transitively (e.g. an alias
	 * A→B and an alias B→C resolves A→C). Idempotent — calling it multiple times is the same as calling it once.
	 *
	 * @return this
	 */
	public AliaserBuilder recursive() {
		this.recursive = true;
		return this;
	}

	/**
	 * @return a single {@link ITableAliaser} composing every registered source, with first-source-wins precedence.
	 *         Returns {@link IdentityImplicitAliaser} when no sources were registered.
	 */
	public ITableAliaser build() {
		ITableAliaser core;
		if (sources.isEmpty()) {
			core = IdentityImplicitAliaser.IDENTITY.get();
		} else if (sources.size() == 1) {
			core = sources.get(0);
		} else {
			CompositeTableAliaser.CompositeTableAliaserBuilder composite = CompositeTableAliaser.builder();
			sources.forEach(composite::aliaser);
			core = composite.build();
		}
		if (recursive) {
			return RecursiveAliaser.wrap(core);
		} else {
			return core;
		}
	}
}
