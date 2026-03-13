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

import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.solven.adhoc.beta.schema.AdhocSchema;
import eu.solven.adhoc.beta.schema.EndpointSchemaMetadata;
import eu.solven.adhoc.beta.schema.IAdhocSchema;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.ListBasedTabularView;
import eu.solven.adhoc.pivotable.endpoint.PivotableAdhocSchemaRegistry;
import eu.solven.adhoc.pivotable.endpoint.PivotableEndpointsRegistry;
import eu.solven.adhoc.query.cube.CubeQuery;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * MCP tools exposing the Pivotable OLAP API to AI agents (e.g. Claude Code).
 *
 * <p>
 * Three tools are provided, meant to be called in order:
 * <ol>
 * <li>{@link #listEndpoints()} — discover available endpoints</li>
 * <li>{@link #getSchema(String)} — inspect cubes, measures and columns for an endpoint</li>
 * <li>{@link #executeQuery(String, String, String)} — run a query and get tabular results</li>
 * </ol>
 *
 * @author Benoit Lacelle
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class PivotableMcpTools {

	final PivotableEndpointsRegistry endpointsRegistry;
	final PivotableAdhocSchemaRegistry schemasRegistry;
	final ObjectMapper objectMapper;

	@Tool(description = "List available OLAP endpoints. " + "Returns one line per endpoint: '<uuid> <name>'. "
			+ "Use the UUID in subsequent getSchema and executeQuery calls.")
	public String listEndpoints() {
		return endpointsRegistry.getEndpoints()
				.map(e -> e.getId() + " " + e.getName())
				.collect(Collectors.joining("\n"));
	}

	@Tool(description = "Get the schema for an endpoint: cubes, measures, dimension columns and sample coordinates. "
			+ "Call listEndpoints first to obtain a valid endpointId.")
	public String getSchema(@ToolParam(description = "Endpoint UUID returned by listEndpoints") String endpointId) {
		AdhocSchema schema = schemasRegistry.getSchema(UUID.fromString(endpointId));
		EndpointSchemaMetadata metadata = schema.getMetadata(IAdhocSchema.AdhocSchemaQuery.builder().build(), true);
		return toJson(metadata);
	}

	@Tool(description = "Execute an OLAP query against a cube and return a tabular result as JSON. "
			+ "Call listEndpoints and getSchema first to discover valid endpointId, cubeName and available measures/columns. "
			+ "queryJson must be a valid CubeQuery JSON, "
			+ "e.g. {\"measureNames\":[\"Revenue.SUM\"],\"groupBy\":[\"Country\"]}")
	@SneakyThrows(JsonProcessingException.class)
	public String executeQuery(@ToolParam(description = "Endpoint UUID returned by listEndpoints") String endpointId,
			@ToolParam(description = "Cube name returned by getSchema") String cubeName,
			@ToolParam(description = "CubeQuery as JSON") String queryJson) {
		CubeQuery query = objectMapper.readValue(queryJson, CubeQuery.class);
		AdhocSchema schema = schemasRegistry.getSchema(UUID.fromString(endpointId));

		log.info("MCP executeQuery endpointId={} cube={} query={}", endpointId, cubeName, query);

		ITabularView result = schema.execute(cubeName, query);
		return toJson(ListBasedTabularView.load(result));
	}

	@SneakyThrows(JsonProcessingException.class)
	private String toJson(Object value) {
		return objectMapper.writeValueAsString(value);
	}
}
