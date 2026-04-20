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
package eu.solven.adhoc.measure.forest;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.filter.AndFilter;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.IAndFilter;
import eu.solven.adhoc.filter.INotFilter;
import eu.solven.adhoc.filter.IOrFilter;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.NotFilter;
import eu.solven.adhoc.filter.OrFilter;
import eu.solven.adhoc.filter.value.AndMatcher;
import eu.solven.adhoc.filter.value.ComparingMatcher;
import eu.solven.adhoc.filter.value.EqualsMatcher;
import eu.solven.adhoc.filter.value.IValueMatcher;
import eu.solven.adhoc.filter.value.InMatcher;
import eu.solven.adhoc.filter.value.LikeMatcher;
import eu.solven.adhoc.filter.value.NotMatcher;
import eu.solven.adhoc.filter.value.OrMatcher;
import eu.solven.adhoc.filter.value.StringMatcher;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Columnator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.model.Filtrator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.model.Partitionor;
import eu.solven.adhoc.measure.model.Shiftor;
import eu.solven.adhoc.measure.model.Unfiltrator;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Produces a "concealed" copy of a {@link IMeasureForest} that is safe to export without leaking trade secrets.
 *
 * <p>
 * Each measure keeps its original type and all standard properties. Only identifiers are replaced:
 * <ul>
 * <li><b>Measure names</b> — replaced by {@code m_<hash>} (hex, length controlled by {@link #hashLength}). All
 * cross-references (underlyings) are rewritten consistently.</li>
 * <li><b>Aggregator.columnName</b> — replaced by {@code c_<hash>}.</li>
 * <li><b>Filtrator.filter</b> — column names inside the filter tree are replaced by {@code c_<hash>}; operand values
 * reachable from every {@link ColumnFilter#getValueMatcher() valueMatcher} ({@code EqualsMatcher}, {@code InMatcher},
 * {@code NotMatcher}, {@code AndMatcher}, {@code OrMatcher}, {@code ComparingMatcher}, {@code LikeMatcher},
 * {@code StringMatcher}) are replaced by {@code v_<hash>} tokens.</li>
 * <li><b>Tags</b> — replaced by {@code t_<hash>}.</li>
 * <li><b>Operator keys</b> ({@code aggregationKey}, {@code combinationKey}, {@code editorKey},
 * {@code decompositionKey}) — replaced by {@code k_<hash>} if the key is <em>not</em> in the matching per-kind
 * whitelist ({@link #standardAggregationKeys}, {@link #standardCombinationKeys}, {@link #standardDecompositionKeys},
 * {@link #standardEditorKeys}). Defaults mirror the keys natively recognised by
 * {@code StandardOperatorFactory.makeAggregation}/{@code makeCombination}/etc. Custom keys (typically fully-qualified
 * class names) are concealed.</li>
 * <li><b>Options</b> (e.g. {@code Combinator.combinationOptions}, {@code Shiftor.editorOptions}) — cleared by default,
 * since they typically contain arbitrary domain-specific data that cannot be safely anonymised; subclasses of
 * {@link ConcealingVisitor} may override {@link AMappingVisitor#mapOptions(Map)} to selectively preserve or transform
 * specific keys.</li>
 * </ul>
 * Within each prefix namespace ({@code m_}, {@code c_}, {@code v_}, {@code k_}, {@code t_}), hash collisions are
 * resolved by appending {@code _2}, {@code _3}, … to the duplicate, with the first occurrence keeping the un-suffixed
 * slot.
 *
 * <p>
 * Instantiate and call {@link #concealWithDefinition(IMeasureForest)} to obtain both the concealed forest and the
 * {@link ConcealingDefinition} required to {@link #restore(IMeasureForest, ConcealingDefinition) restore} it later.
 * Subclasses may override the {@code protected build*Mapping} methods or {@link AMappingVisitor#mapOptions(Map)} to
 * customise behaviour.
 *
 * @author Benoit Lacelle
 */
@Builder
@Slf4j
@SuppressWarnings("PMD.GodClass")
public class MeasureForestConcealer {

	private static final int BITS_PER_HEXDIGIT = 4;

	/**
	 * Aggregation keys natively recognised by {@code StandardOperatorFactory.makeAggregation} — each one hits an
	 * explicit switch case there (not the {@code default} → {@code Class.forName} branch). They carry no secret and are
	 * kept verbatim during concealment.
	 */
	public static final ImmutableSet<String> DEFAULT_STANDARD_AGGREGATION_KEYS = ImmutableSet.of("SUM",
			"SUM_NOT_NAN",
			"MAX",
			"MIN",
			"COUNT",
			"expression",
			"EMPTY",
			"RANK",
			"AVG",
			"PRODUCT",
			"COALESCE");

	/**
	 * Combination keys natively recognised by {@code StandardOperatorFactory.makeCombination} — either through one of
	 * the {@code isSum} / {@code isProduct} / {@code isDivide} / {@code isSubstraction} helpers or through an explicit
	 * switch case. {@code EXPRESSION} (EvaluatedExpressionCombination) is intentionally absent because its
	 * instantiation requires the optional {@code com.ezylang.evalex} dependency.
	 */
	public static final ImmutableSet<String> DEFAULT_STANDARD_COMBINATION_KEYS =
			ImmutableSet.of("SUM", "+", "PRODUCT", "*", "DIVIDE", "/", "SUBSTRACTION", "-", "MAX", "MIN", "COALESCE");

	/**
	 * Decomposition keys natively recognised by {@code StandardOperatorFactory.makeDecomposition}.
	 */
	public static final ImmutableSet<String> DEFAULT_STANDARD_DECOMPOSITION_KEYS =
			ImmutableSet.of("identity", "linear");

	/**
	 * Filter-editor keys natively recognised by {@code StandardOperatorFactory.makeEditor}.
	 */
	public static final ImmutableSet<String> DEFAULT_STANDARD_EDITOR_KEYS = ImmutableSet.of("identity", "simple");

	/**
	 * Number of hex digits in each concealed token. Defaults to {@code 8} (32-bit hash, e.g. {@code m_a3f1c09e}).
	 * Shorter values reduce output verbosity at the cost of higher collision probability; longer values increase
	 * uniqueness (up to the 8-digit limit imposed by {@link String#hashCode()}'s 32-bit range).
	 */
	@Builder.Default
	protected final int hashLength = 8;

	/**
	 * Aggregation keys kept verbatim during concealment. Defaults to {@link #DEFAULT_STANDARD_AGGREGATION_KEYS};
	 * override via the builder to align with a custom {@code IOperatorFactory}.
	 */
	@Builder.Default
	protected final Set<String> standardAggregationKeys = DEFAULT_STANDARD_AGGREGATION_KEYS;

	/**
	 * Combination keys kept verbatim during concealment. Defaults to {@link #DEFAULT_STANDARD_COMBINATION_KEYS}.
	 */
	@Builder.Default
	protected final Set<String> standardCombinationKeys = DEFAULT_STANDARD_COMBINATION_KEYS;

	/**
	 * Decomposition keys kept verbatim during concealment. Defaults to {@link #DEFAULT_STANDARD_DECOMPOSITION_KEYS}.
	 */
	@Builder.Default
	protected final Set<String> standardDecompositionKeys = DEFAULT_STANDARD_DECOMPOSITION_KEYS;

	/**
	 * Filter-editor keys kept verbatim during concealment. Defaults to {@link #DEFAULT_STANDARD_EDITOR_KEYS}.
	 */
	@Builder.Default
	protected final Set<String> standardEditorKeys = DEFAULT_STANDARD_EDITOR_KEYS;

	/**
	 * Holds all mapping tables produced during a concealment pass. A safe party can use this together with
	 * {@link MeasureForestConcealer#restore(IMeasureForest, ConcealingDefinition)} to recover the original forest.
	 */
	@Value
	public static class ConcealingDefinition {
		/** original measure name → concealed name */
		Map<String, String> nameMapping;
		/** original column name → concealed name */
		Map<String, String> columnMapping;
		/** original operand value (any type) → concealed {@code v_<hash>} token */
		Map<Object, String> valueMapping;
		/** original operator key → concealed {@code k_<hash>} token (only for non-standard keys) */
		Map<String, String> keyMapping;
		/** original tag → concealed tag */
		Map<String, String> tagMapping;
	}

	/**
	 * Bundles the concealed forest together with the {@link ConcealingDefinition} needed to restore it.
	 */
	@Value
	public static class ConcealingResult {
		IMeasureForest concealedForest;
		ConcealingDefinition definition;
	}

	/**
	 * Returns a concealed copy of {@code forest}. The original forest is not modified.
	 *
	 * <p>
	 * Use {@link #concealWithDefinition(IMeasureForest)} when the {@link ConcealingDefinition} is also needed.
	 */
	public IMeasureForest conceal(IMeasureForest forest) {
		return concealWithDefinition(forest).getConcealedForest();
	}

	/**
	 * Returns both the concealed forest and the {@link ConcealingDefinition} that documents every substitution made.
	 * The definition can later be passed to {@link #restore(IMeasureForest, ConcealingDefinition)} by a trusted party
	 * to recover the original forest.
	 */
	public ConcealingResult concealWithDefinition(IMeasureForest forest) {
		Collection<IMeasure> measures = forest.getNameToMeasure().values();

		// Include both measure names and the forest name so restoration can invert the mapping.
		Set<String> allNames = new LinkedHashSet<>();
		allNames.add(forest.getName());
		allNames.addAll(forest.getNameToMeasure().keySet());
		Map<String, String> nameMapping = buildNameMapping(allNames);

		Set<String> columnNames = new LinkedHashSet<>();
		Set<String> tagNames = new LinkedHashSet<>();
		Set<Object> operandValues = new LinkedHashSet<>();
		Set<String> concealableKeys = new LinkedHashSet<>();
		for (IMeasure m : measures) {
			tagNames.addAll(m.getTags());
			collectConcealableKeys(m, concealableKeys);
			if (m instanceof Aggregator agg) {
				columnNames.add(agg.getColumnName());
			}
			if (m instanceof Filtrator fil) {
				collectFilterColumns(fil.getFilter(), columnNames);
				collectFilterOperands(fil.getFilter(), operandValues);
			}
			if (m instanceof Columnator col) {
				columnNames.addAll(col.getColumns());
			}
			if (m instanceof Partitionor par) {
				columnNames.addAll(par.getGroupBy().getSortedColumns());
			}
		}
		Map<String, String> columnMapping = buildColumnMapping(columnNames);
		Map<Object, String> valueMapping = buildValueMapping(operandValues);
		Map<String, String> keyMapping = buildKeyMapping(concealableKeys);
		Map<String, String> tagMapping = buildTagMapping(tagNames);

		ConcealingDefinition definition =
				new ConcealingDefinition(nameMapping, columnMapping, valueMapping, keyMapping, tagMapping);
		IMeasureForest concealedForest = forest.acceptVisitor(new ConcealingVisitor(nameMapping,
				columnMapping,
				valueMapping,
				keyMapping,
				tagMapping,
				standardAggregationKeys,
				standardCombinationKeys,
				standardDecompositionKeys,
				standardEditorKeys));
		return new ConcealingResult(concealedForest, definition);
	}

	/**
	 * Restores a previously concealed forest by reversing all substitutions recorded in {@code definition}.
	 *
	 * <p>
	 * The restored forest is structurally identical to the original; only names and identifiers differ in a freshly
	 * built object graph.
	 *
	 * @param concealedForest
	 *            the forest produced by {@link #conceal(IMeasureForest)} or
	 *            {@link #concealWithDefinition(IMeasureForest)}
	 * @param definition
	 *            the definition returned alongside the concealed forest
	 * @return a new forest whose identifiers match the originals
	 */
	public IMeasureForest restore(IMeasureForest concealedForest, ConcealingDefinition definition) {
		return concealedForest.acceptVisitor(new AMappingVisitor(invert(definition.getNameMapping()),
				invert(definition.getColumnMapping()),
				invertValueMapping(definition.getValueMapping()),
				invert(definition.getKeyMapping()),
				invert(definition.getTagMapping()),
				standardAggregationKeys,
				standardCombinationKeys,
				standardDecompositionKeys,
				standardEditorKeys));
	}

	// ── mapping builders ──────────────────────────────────────────────────────

	/**
	 * Builds a deterministic {@code oldName → newName} mapping using the {@code m_} prefix.
	 *
	 * @see #buildMapping(String, Set)
	 */
	protected Map<String, String> buildNameMapping(Set<String> names) {
		return buildMapping("m_", names);
	}

	/**
	 * Builds a deterministic {@code oldColumnName → newColumnName} mapping using the {@code c_} prefix.
	 *
	 * @see #buildMapping(String, Set)
	 */
	protected Map<String, String> buildColumnMapping(Set<String> columnNames) {
		return buildMapping("c_", columnNames);
	}

	/**
	 * Builds a deterministic {@code oldValue → concealed token} mapping using the {@code v_} prefix. Keys may be any
	 * {@link Object} — the hash is computed from {@link Objects#hashCode(Object)} so built-in boxed types (String,
	 * Integer, Long, Double, Boolean, …) produce stable tokens across JVMs.
	 *
	 * @see #buildMapping(String, Set)
	 */
	protected Map<Object, String> buildValueMapping(Set<Object> values) {
		return buildObjectMapping("v_", values);
	}

	/**
	 * Builds a deterministic {@code oldKey → newKey} mapping using the {@code k_} prefix over already-filtered
	 * {@code concealableKeys} (standard keys have already been removed by
	 * {@link #collectConcealableKeys(IMeasure, Set)}).
	 *
	 * @see #buildMapping(String, Set)
	 */
	protected Map<String, String> buildKeyMapping(Set<String> concealableKeys) {
		return buildMapping("k_", concealableKeys);
	}

	/**
	 * Builds a deterministic {@code oldTag → newTag} mapping using the {@code t_} prefix.
	 *
	 * @see #buildMapping(String, Set)
	 */
	protected Map<String, String> buildTagMapping(Set<String> tags) {
		return buildMapping("t_", tags);
	}

	/**
	 * Builds a deterministic {@code original → concealed} mapping over the given name set.
	 *
	 * <p>
	 * Each name is mapped to {@code <prefix><hashLength-hex-digit hash>} (unsigned lower-case hex of
	 * {@link String#hashCode()}, truncated to {@link #hashLength} hex digits). When two names produce the same base
	 * hash the second receives a {@code _2} suffix, the third {@code _3}, and so on. Iteration order of {@code names}
	 * determines which name wins the un-suffixed slot.
	 *
	 * @param prefix
	 *            the token prepended to the hex hash: {@code "m_"} for measure names, {@code "c_"} for column names,
	 *            {@code "v_"} for values, {@code "k_"} for operator keys, {@code "t_"} for tags
	 * @param names
	 *            the original names in a stable iteration order
	 * @return an unmodifiable map from every original name to its concealed replacement
	 */
	protected Map<String, String> buildMapping(String prefix, Set<String> names) {
		Map<String, String> oldToNew = new LinkedHashMap<>();
		Map<String, Integer> baseCount = new LinkedHashMap<>();
		long mask = (1L << (hashLength * BITS_PER_HEXDIGIT)) - 1;
		String fmt = "%0" + hashLength + "x";

		for (String name : names) {
			String base = prefix + String.format(fmt, name.hashCode() & mask);
			int count = baseCount.merge(base, 1, Integer::sum);
			String mapped;
			if (count == 1) {
				mapped = base;
			} else {
				mapped = base + "_" + count;
			}
			oldToNew.put(name, mapped);
		}
		return Collections.unmodifiableMap(oldToNew);
	}

	/**
	 * Variant of {@link #buildMapping(String, Set)} that accepts arbitrary {@link Object} operands — used for filter
	 * value operands which may be boxed numbers, booleans, strings, dates, etc. Hashing goes through
	 * {@link Objects#hashCode(Object)}.
	 */
	protected Map<Object, String> buildObjectMapping(String prefix, Set<Object> values) {
		Map<Object, String> oldToNew = new LinkedHashMap<>();
		Map<String, Integer> baseCount = new LinkedHashMap<>();
		long mask = (1L << (hashLength * BITS_PER_HEXDIGIT)) - 1;
		String fmt = "%0" + hashLength + "x";

		for (Object value : values) {
			String base = prefix + String.format(fmt, Objects.hashCode(value) & mask);
			int count = baseCount.merge(base, 1, Integer::sum);
			String mapped;
			if (count == 1) {
				mapped = base;
			} else {
				mapped = base + "_" + count;
			}
			oldToNew.put(value, mapped);
		}
		return Collections.unmodifiableMap(oldToNew);
	}

	// ── filter / utility helpers ──────────────────────────────────────────────

	/**
	 * Recursively collects every column name referenced inside {@code filter} into {@code columns}.
	 *
	 * <p>
	 * This method is intentionally {@code static}: it operates purely on {@link ISliceFilter} with no dependency on
	 * concealer state.
	 */
	static void collectFilterColumns(ISliceFilter filter, Set<String> columns) {
		if (filter instanceof ColumnFilter cf) {
			columns.add(cf.getColumn());
		} else if (filter instanceof IAndFilter af) {
			af.getOperands().forEach(f -> collectFilterColumns(f, columns));
		} else if (filter instanceof IOrFilter of) {
			of.getOperands().forEach(f -> collectFilterColumns(f, columns));
		} else if (filter instanceof INotFilter nf) {
			collectFilterColumns(nf.getNegated(), columns);
		}
	}

	/**
	 * Recursively collects every operand value reachable through {@link ColumnFilter#getValueMatcher()} into
	 * {@code values}. Structural matchers (null, MATCH_ALL, MATCH_NONE) and the regex matcher contribute nothing.
	 */
	static void collectFilterOperands(ISliceFilter filter, Set<Object> values) {
		if (filter instanceof ColumnFilter cf) {
			collectMatcherOperands(cf.getValueMatcher(), values);
		} else if (filter instanceof IAndFilter af) {
			af.getOperands().forEach(f -> collectFilterOperands(f, values));
		} else if (filter instanceof IOrFilter of) {
			of.getOperands().forEach(f -> collectFilterOperands(f, values));
		} else if (filter instanceof INotFilter nf) {
			collectFilterOperands(nf.getNegated(), values);
		}
	}

	/**
	 * Walks a {@link IValueMatcher} tree and adds every concealable operand value to {@code values}.
	 */
	static void collectMatcherOperands(IValueMatcher matcher, Set<Object> values) {
		if (matcher instanceof EqualsMatcher em) {
			values.add(em.getOperand());
		} else if (matcher instanceof InMatcher im) {
			values.addAll(im.getOperands());
		} else if (matcher instanceof NotMatcher nm) {
			collectMatcherOperands(nm.getNegated(), values);
		} else if (matcher instanceof AndMatcher am) {
			am.getOperands().forEach(m -> collectMatcherOperands(m, values));
		} else if (matcher instanceof OrMatcher om) {
			om.getOperands().forEach(m -> collectMatcherOperands(m, values));
		} else if (matcher instanceof ComparingMatcher cm) {
			values.add(cm.getOperand());
		} else if (matcher instanceof LikeMatcher lm) {
			values.add(lm.getPattern());
		} else if (matcher instanceof StringMatcher sm) {
			values.add(sm.getString());
		}
		// NullMatcher, SameMatcher, RegexMatcher → no concealable operand (structural / pre-compiled Pattern).
	}

	/**
	 * Collects every operator key declared by the given measure that is <em>not</em> in the matching per-kind whitelist
	 * ({@link #standardAggregationKeys}, {@link #standardCombinationKeys}, etc.). Each key is checked against the kind
	 * of the field it lives on — so {@code "SUM"} on an {@code aggregationKey} is recognised via
	 * {@link #standardAggregationKeys} while {@code "SUM"} on a {@code combinationKey} is recognised via
	 * {@link #standardCombinationKeys}.
	 */
	protected void collectConcealableKeys(IMeasure measure, Set<String> concealableKeys) {
		if (measure instanceof Aggregator agg) {
			addIfCustom(agg.getAggregationKey(), standardAggregationKeys, concealableKeys);
		} else if (measure instanceof Combinator comb) {
			addIfCustom(comb.getCombinationKey(), standardCombinationKeys, concealableKeys);
		} else if (measure instanceof Columnator col) {
			addIfCustom(col.getCombinationKey(), standardCombinationKeys, concealableKeys);
		} else if (measure instanceof Partitionor par) {
			addIfCustom(par.getAggregationKey(), standardAggregationKeys, concealableKeys);
			addIfCustom(par.getCombinationKey(), standardCombinationKeys, concealableKeys);
		} else if (measure instanceof Dispatchor dis) {
			addIfCustom(dis.getAggregationKey(), standardAggregationKeys, concealableKeys);
			addIfCustom(dis.getDecompositionKey(), standardDecompositionKeys, concealableKeys);
		} else if (measure instanceof Shiftor shiftor) {
			addIfCustom(shiftor.getEditorKey(), standardEditorKeys, concealableKeys);
		}
	}

	private static void addIfCustom(String key, Set<String> whitelist, Set<String> out) {
		if (!whitelist.contains(key)) {
			out.add(key);
		}
	}

	private static Map<String, String> invert(Map<String, String> mapping) {
		Map<String, String> inverted = new LinkedHashMap<>();
		mapping.forEach((k, v) -> inverted.put(v, k));
		return Collections.unmodifiableMap(inverted);
	}

	private static Map<Object, Object> invertValueMapping(Map<Object, String> mapping) {
		Map<Object, Object> inverted = new LinkedHashMap<>();
		mapping.forEach((k, v) -> inverted.put(v, k));
		return Collections.unmodifiableMap(inverted);
	}

	// ── AMappingVisitor ───────────────────────────────────────────────────────

	/**
	 * A {@link IMeasureForestVisitor} that rewrites every identifier in a forest by looking it up in five {@link Map}s:
	 * one for names (measures + forest), one for column names, one for operand values, one for operator keys, one for
	 * tags.
	 *
	 * <p>
	 * Both concealment and restoration share this implementation — they differ only in which maps are passed in
	 * (forward hashes for concealment, inverted maps for restoration). For the value map, entries are typed
	 * {@code Map<Object, Object>} so the same field holds both {@code original → concealed-String} (forward) and
	 * {@code concealed-String → original} (inverse).
	 *
	 * <p>
	 * Subclasses may override {@link #mapOptions(Map)} to customise how option maps (e.g.
	 * {@code Shiftor.editorOptions}) are transformed.
	 */
	protected static class AMappingVisitor implements IMeasureForestVisitor {

		protected final Map<String, String> nameMapping;
		protected final Map<String, String> columnMapping;
		protected final Map<Object, Object> valueMapping;
		protected final Map<String, String> keyMapping;
		protected final Map<String, String> tagMapping;
		protected final Set<String> standardAggregationKeys;
		protected final Set<String> standardCombinationKeys;
		protected final Set<String> standardDecompositionKeys;
		protected final Set<String> standardEditorKeys;

		protected AMappingVisitor(Map<String, String> nameMapping,
				Map<String, String> columnMapping,
				Map<Object, Object> valueMapping,
				Map<String, String> keyMapping,
				Map<String, String> tagMapping,
				Set<String> standardAggregationKeys,
				Set<String> standardCombinationKeys,
				Set<String> standardDecompositionKeys,
				Set<String> standardEditorKeys) {
			this.nameMapping = nameMapping;
			this.columnMapping = columnMapping;
			this.valueMapping = valueMapping;
			this.keyMapping = keyMapping;
			this.tagMapping = tagMapping;
			this.standardAggregationKeys = standardAggregationKeys;
			this.standardCombinationKeys = standardCombinationKeys;
			this.standardDecompositionKeys = standardDecompositionKeys;
			this.standardEditorKeys = standardEditorKeys;
		}

		@Override
		public String editName(String name) {
			return nameMapping.getOrDefault(name, name);
		}

		@Override
		public Set<IMeasure> mapMeasure(IMeasure measure) {
			String newName = mapName(measure.getName());
			IMeasure mapped = doMapMeasure(measure, newName);
			return ImmutableSet.of(mapped.withTags(mapTagSet(measure.getTags())));
		}

		private IMeasure doMapMeasure(IMeasure measure, String newName) {
			if (measure instanceof Aggregator agg) {
				return agg.toBuilder()
						.name(newName)
						.columnName(columnMapping.getOrDefault(agg.getColumnName(), agg.getColumnName()))
						.aggregationKey(mapAggregationKey(agg.getAggregationKey()))
						.clearAggregationOptions()
						.aggregationOptions(mapOptions(agg.getAggregationOptions()))
						.build();
			} else if (measure instanceof Combinator comb) {
				return comb.toBuilder()
						.name(newName)
						.clearUnderlyings()
						.underlyings(mapNames(comb.getUnderlyings()))
						.combinationKey(mapCombinationKey(comb.getCombinationKey()))
						.clearCombinationOptions()
						.combinationOptions(mapOptions(comb.getCombinationOptions()))
						.build();
			} else if (measure instanceof Columnator col) {
				return col.toBuilder()
						.name(newName)
						.clearColumns()
						.columns(col.getColumns()
								.stream()
								.map(c -> columnMapping.getOrDefault(c, c))
								.collect(ImmutableSet.toImmutableSet()))
						.clearUnderlyings()
						.underlyings(mapNames(col.getUnderlyings()))
						.combinationKey(mapCombinationKey(col.getCombinationKey()))
						.clearCombinationOptions()
						.combinationOptions(mapOptions(col.getCombinationOptions()))
						.build();
			} else if (measure instanceof Filtrator fil) {
				return fil.toBuilder()
						.name(newName)
						.underlying(mapName(fil.getUnderlying()))
						.filter(mapFilter(fil.getFilter()))
						.build();
			} else if (measure instanceof Shiftor shiftor) {
				return shiftor.toBuilder()
						.name(newName)
						.underlying(mapName(shiftor.getUnderlying()))
						.editorKey(mapEditorKey(shiftor.getEditorKey()))
						.clearEditorOptions()
						.editorOptions(mapOptions(shiftor.getEditorOptions()))
						.build();
			} else if (measure instanceof Dispatchor dis) {
				return dis.toBuilder()
						.name(newName)
						.underlying(mapName(dis.getUnderlying()))
						.aggregationKey(mapAggregationKey(dis.getAggregationKey()))
						.decompositionKey(mapDecompositionKey(dis.getDecompositionKey()))
						.clearAggregationOptions()
						.aggregationOptions(mapOptions(dis.getAggregationOptions()))
						.clearDecompositionOptions()
						.decompositionOptions(mapOptions(dis.getDecompositionOptions()))
						.build();
			} else if (measure instanceof Partitionor par) {
				return par.toBuilder()
						.name(newName)
						.clearUnderlyings()
						.underlyings(mapNames(par.getUnderlyings()))
						.groupBy(mapGroupBy(par.getGroupBy()))
						.aggregationKey(mapAggregationKey(par.getAggregationKey()))
						.combinationKey(mapCombinationKey(par.getCombinationKey()))
						.clearAggregationOptions()
						.aggregationOptions(mapOptions(par.getAggregationOptions()))
						.clearCombinationOptions()
						.combinationOptions(mapOptions(par.getCombinationOptions()))
						.build();
			} else if (measure instanceof Unfiltrator unf) {
				return unf.toBuilder().name(newName).underlying(mapName(unf.getUnderlying())).build();
			}

			log.warn("MeasureForestConcealer: unknown IMeasure type {} — identifiers not mapped",
					measure.getClass().getName());
			return measure;
		}

		/**
		 * Recursively rewrites every {@link ColumnFilter#getColumn() column} and {@link ColumnFilter#getValueMatcher()
		 * value-matcher operand} inside {@code filter}.
		 */
		protected ISliceFilter mapFilter(ISliceFilter filter) {
			if (filter instanceof ColumnFilter cf) {
				return cf.toBuilder()
						.column(columnMapping.getOrDefault(cf.getColumn(), cf.getColumn()))
						.valueMatcher(mapValueMatcher(cf.getValueMatcher()))
						.build();
			}
			if (filter instanceof IAndFilter af) {
				List<ISliceFilter> mapped = af.getOperands().stream().map(this::mapFilter).collect(Collectors.toList());
				return AndFilter.copyOf(mapped);
			}
			if (filter instanceof IOrFilter of) {
				List<ISliceFilter> mapped = of.getOperands().stream().map(this::mapFilter).collect(Collectors.toList());
				return OrFilter.copyOf(mapped);
			}
			if (filter instanceof INotFilter nf) {
				return NotFilter.builder().negated(mapFilter(nf.getNegated())).build();
			}
			// Structural filters (MATCH_ALL, MATCH_NONE) carry no column → return as-is.
			return filter;
		}

		/**
		 * Rewrites every operand reachable inside {@code matcher} using {@link #valueMapping}, preserving logical
		 * structure (AND / OR / NOT). Structural matchers (null, MATCH_ALL, MATCH_NONE, RegexMatcher) are returned
		 * as-is.
		 */
		protected IValueMatcher mapValueMatcher(IValueMatcher matcher) {
			if (matcher == IValueMatcher.MATCH_ALL || matcher == IValueMatcher.MATCH_NONE) {
				return matcher;
			}
			if (matcher instanceof EqualsMatcher em) {
				Object original = em.getOperand();
				Object mapped = valueMapping.getOrDefault(original, original);
				return EqualsMatcher.matchEq(mapped);
			}
			if (matcher instanceof InMatcher im) {
				ImmutableSet<Object> mapped = im.getOperands()
						.stream()
						.map(o -> valueMapping.getOrDefault(o, o))
						.collect(ImmutableSet.toImmutableSet());
				return InMatcher.builder().operands(mapped).build();
			}
			if (matcher instanceof NotMatcher nm) {
				return NotMatcher.builder().negated(mapValueMatcher(nm.getNegated())).build();
			}
			if (matcher instanceof AndMatcher am) {
				return AndMatcher.copyOf(am.getOperands().stream().map(this::mapValueMatcher).toList());
			}
			if (matcher instanceof OrMatcher om) {
				return OrMatcher.copyOf(om.getOperands().stream().map(this::mapValueMatcher).toList());
			}
			if (matcher instanceof ComparingMatcher cm) {
				Object mapped = valueMapping.getOrDefault(cm.getOperand(), cm.getOperand());
				return cm.toBuilder().operand(mapped).build();
			}
			if (matcher instanceof LikeMatcher lm) {
				Object mapped = valueMapping.getOrDefault(lm.getPattern(), lm.getPattern());
				return LikeMatcher.builder().pattern(String.valueOf(mapped)).build();
			}
			if (matcher instanceof StringMatcher sm) {
				Object mapped = valueMapping.getOrDefault(sm.getString(), sm.getString());
				return StringMatcher.builder().string(String.valueOf(mapped)).build();
			}
			return matcher;
		}

		/**
		 * Transforms an option map (e.g. {@code Shiftor.editorOptions}, {@code Combinator.combinationOptions}).
		 *
		 * <p>
		 * The default implementation clears all options, since they typically contain arbitrary domain-specific data
		 * that cannot be safely anonymised. Subclasses may override this method to selectively preserve or transform
		 * specific keys.
		 *
		 * @param options
		 *            the original options map (never {@code null})
		 * @return the options to write into the mapped measure
		 */
		protected Map<String, ?> mapOptions(Map<String, ?> options) {
			return Collections.emptyMap();
		}

		protected IGroupBy mapGroupBy(IGroupBy groupBy) {
			if (groupBy.isGrandTotal()) {
				return IGroupBy.GRAND_TOTAL;
			}
			return GroupByColumns
					.named(groupBy.getSortedColumns().stream().map(c -> columnMapping.getOrDefault(c, c)).toList());
		}

		protected String mapName(String name) {
			return nameMapping.getOrDefault(name, name);
		}

		/**
		 * Per-kind operator-key mapping. If {@code key} is in the matching whitelist it passes through verbatim;
		 * otherwise the {@link #keyMapping} concealment/restoration lookup applies.
		 */
		protected String mapAggregationKey(String key) {
			return mapKey(key, standardAggregationKeys);
		}

		protected String mapCombinationKey(String key) {
			return mapKey(key, standardCombinationKeys);
		}

		protected String mapDecompositionKey(String key) {
			return mapKey(key, standardDecompositionKeys);
		}

		protected String mapEditorKey(String key) {
			return mapKey(key, standardEditorKeys);
		}

		private String mapKey(String key, Set<String> whitelist) {
			if (whitelist.contains(key)) {
				return key;
			}
			return keyMapping.getOrDefault(key, key);
		}

		private List<String> mapNames(List<String> names) {
			return names.stream().map(this::mapName).collect(ImmutableList.toImmutableList());
		}

		private ImmutableSet<String> mapTagSet(Set<String> tags) {
			return tags.stream().map(t -> tagMapping.getOrDefault(t, t)).collect(ImmutableSet.toImmutableSet());
		}
	}

	// ── ConcealingVisitor ──────────────────────────────────────────────────────

	/**
	 * Specialisation of {@link AMappingVisitor} for concealment. Widens the forward {@code Map<Object, String>}
	 * value-mapping to the visitor's {@code Map<Object, Object>} contract.
	 */
	protected static class ConcealingVisitor extends AMappingVisitor {

		ConcealingVisitor(Map<String, String> nameMapping,
				Map<String, String> columnMapping,
				Map<Object, String> valueMapping,
				Map<String, String> keyMapping,
				Map<String, String> tagMapping,
				Set<String> standardAggregationKeys,
				Set<String> standardCombinationKeys,
				Set<String> standardDecompositionKeys,
				Set<String> standardEditorKeys) {
			super(nameMapping,
					columnMapping,
					widen(valueMapping),
					keyMapping,
					tagMapping,
					standardAggregationKeys,
					standardCombinationKeys,
					standardDecompositionKeys,
					standardEditorKeys);
		}

		private static Map<Object, Object> widen(Map<Object, String> forward) {
			Map<Object, Object> widened = new LinkedHashMap<>();
			forward.forEach(widened::put);
			return Collections.unmodifiableMap(widened);
		}
	}
}
