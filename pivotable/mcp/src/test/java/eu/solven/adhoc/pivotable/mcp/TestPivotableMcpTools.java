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
package eu.solven.adhoc.pivotable.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.solven.adhoc.beta.schema.AdhocSchema;
import eu.solven.adhoc.beta.schema.EndpointSchemaMetadata;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.pivotable.endpoint.PivotableAdhocEndpointMetadata;
import eu.solven.adhoc.pivotable.endpoint.PivotableAdhocSchemaRegistry;
import eu.solven.adhoc.pivotable.endpoint.PivotableEndpointsRegistry;

public class TestPivotableMcpTools {

	PivotableEndpointsRegistry endpointsRegistry = new PivotableEndpointsRegistry();
	PivotableAdhocSchemaRegistry schemasRegistry = new PivotableAdhocSchemaRegistry();
	ObjectMapper objectMapper = new ObjectMapper();

	PivotableMcpTools tools = new PivotableMcpTools(endpointsRegistry, schemasRegistry, objectMapper);

	PivotableAdhocEndpointMetadata endpoint = PivotableAdhocEndpointMetadata.localhost();
	AdhocSchema mockSchema = mock(AdhocSchema.class);

	@BeforeEach
	void registerEndpoint() {
		endpointsRegistry.registerEntrypoint(endpoint);
		schemasRegistry.registerEntrypoint(endpoint.getId(), mockSchema);
	}

	@Test
	public void listEndpoints_returnsIdAndName() {
		String result = tools.listEndpoints();

		assertThat(result).contains(PivotableAdhocEndpointMetadata.SELF_ENTRYPOINT_ID.toString())
				.contains("localhost:self");
	}

	@Test
	public void getSchema_returnsJsonWithCubesAndTables() {
		when(mockSchema.getMetadata(any(), anyBoolean())).thenReturn(EndpointSchemaMetadata.builder().build());

		String result = tools.getSchema(endpoint.getId().toString());

		assertThat(result).contains("cubes").contains("tables");
	}

	@Test
	public void getSchema_unknownEndpoint_throws() {
		assertThatThrownBy(() -> tools.getSchema("00000000-0000-0000-0000-000000000099"))
				.isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	public void executeQuery_parsesQueryJsonAndReturnsTabularResult() {
		when(mockSchema.execute(eq("testCube"), any())).thenReturn(mock(ITabularView.class));

		String result = tools.executeQuery(endpoint.getId().toString(), "testCube", "{\"measureNames\":[]}");

		assertThat(result).isNotEmpty();
	}

	@Test
	public void executeQuery_invalidJson_throws() {
		assertThatThrownBy(() -> tools.executeQuery(endpoint.getId().toString(), "testCube", "not-json"))
				.isInstanceOf(Exception.class);
	}
}
