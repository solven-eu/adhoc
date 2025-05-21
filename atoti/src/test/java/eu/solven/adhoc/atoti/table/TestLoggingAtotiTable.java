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
package eu.solven.adhoc.atoti.table;

import org.junit.jupiter.api.Test;

import com.quartetfs.biz.pivot.query.impl.QueryExtendedPlugin;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.contributions.impl.AnnotationContributionProvider;

import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV2;

public class TestLoggingAtotiTable {
	static {
		Registry.setContributionProvider(new AnnotationContributionProvider(QueryExtendedPlugin.class));
	}

	// Will generate many WARNs due to improper use of javassist in TransferCompiler.compile
	@Test
	public void testNew() {
		LoggingAtotiTable table = LoggingAtotiTable.builder().pivotId("someCubeName").build();

		table.streamSlices(QueryPod.forTable(table),
				TableQueryV2.builder()
						.aggregator(FilteredAggregator.builder().aggregator(Aggregator.sum("v")).build())
						.build());
	}
}
