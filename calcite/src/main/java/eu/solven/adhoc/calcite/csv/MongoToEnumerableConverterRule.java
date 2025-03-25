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
package eu.solven.adhoc.calcite.csv;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/**
 * Rule to convert a relational expression from {@link AdhocCalciteRel#CONVENTION} to {@link EnumerableConvention}.
 */
public class MongoToEnumerableConverterRule extends ConverterRule {
	/** Singleton instance of MongoToEnumerableConverterRule. */
	public static final ConverterRule INSTANCE = Config.INSTANCE
			.withConversion(RelNode.class,
					AdhocCalciteRel.CONVENTION,
					EnumerableConvention.INSTANCE,
					"MongoToEnumerableConverterRule")
			.withRuleFactory(MongoToEnumerableConverterRule::new)
			.toRule(MongoToEnumerableConverterRule.class);

	/** Called from the Config. */
	protected MongoToEnumerableConverterRule(Config config) {
		super(config);
	}

	@Override
	public RelNode convert(RelNode rel) {
		RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
		return new MongoToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
	}
}