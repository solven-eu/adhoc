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
package eu.solven.adhoc.query;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.query.cube.AdhocSubQuery;
import eu.solven.adhoc.query.cube.CubeQuery;

public class TestAdhocQueryId {
	@Test
	public void testQueryId() {
		AdhocQueryId queryId = AdhocQueryId.from("someCube", CubeQuery.builder().build());

		Assertions.assertThat(queryId.getCube()).isEqualTo("someCube");
		Assertions.assertThat(queryId.getParentQueryId()).isNull();
		Assertions.assertThat(queryId.getQueryId()).isNotNull();
	}

	@Test
	public void testQueryId_withparent() {
		AdhocQueryId queryId = AdhocQueryId.from("parentCube", CubeQuery.builder().build());

		AdhocQueryId subQueryId = AdhocQueryId.from("subCube",
				AdhocSubQuery.builder().subQuery(CubeQuery.builder().build()).parentQueryId(queryId).build());

		Assertions.assertThat(subQueryId.getCube()).isEqualTo("subCube");
		Assertions.assertThat(subQueryId.getParentQueryId()).isEqualTo(queryId.getQueryId());
		Assertions.assertThat(subQueryId.getQueryId()).isNotNull().isNotEqualTo(queryId.getQueryId());
	}
}
