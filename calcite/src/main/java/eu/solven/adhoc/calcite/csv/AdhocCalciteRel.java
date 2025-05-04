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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;

import eu.solven.adhoc.query.cube.CubeQuery;

/**
 * Relational expression that uses Mongo calling convention.
 */
public interface AdhocCalciteRel extends RelNode {
	void implement(AdhocCalciteRelImplementor implementor);

	/** Calling convention for relational operations that occur in MongoDB. */
	Convention CONVENTION = new Convention.Impl("ADHOC", AdhocCalciteRel.class);

	/**
	 * Callback for the implementation process that converts a tree of {@link AdhocCalciteRel} nodes into a MongoDB
	 * query.
	 */
	class AdhocCalciteRelImplementor {
		final CubeQuery.CubeQueryBuilder cubeQueryBuilder = CubeQuery.builder();

		final Map<String, String> projects = new LinkedHashMap<>();

		final RexBuilder rexBuilder;
		@Nullable
		RelOptTable table;
		@Nullable
		AdhocCalciteTable adhocTable;

		public AdhocCalciteRelImplementor(RexBuilder rexBuilder) {
			this.rexBuilder = rexBuilder;
		}

		public void visitChild(int ordinal, RelNode input) {
			assert ordinal == 0;
			((AdhocCalciteRel) input).implement(this);
		}

		public void clearProject() {
			projects.clear();
		}
	}
}