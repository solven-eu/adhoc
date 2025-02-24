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
package eu.solven.adhoc.pivotable.query;

import java.util.List;

import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

import eu.solven.adhoc.beta.schema.AdhocSchemaForApi;
import eu.solven.adhoc.beta.schema.TargetedAdhocQuery;
import eu.solven.adhoc.pivotable.webflux.api.AdhocHandlerHelper;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.OrMatcher;
import eu.solven.adhoc.storage.ListBasedTabularView;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
@Slf4j
public class PivotableQueryHandler {
	final AdhocSchemaForApi schema;

	public Mono<ServerResponse> loadMetadata(ServerRequest serverRequest) {
		return ServerResponse.ok()
				.contentType(MediaType.APPLICATION_JSON)
				.body(BodyInserters.fromValue(schema.getMetadata()));
	}

	private Mono<ServerResponse> executeQuery(Mono<TargetedAdhocQuery> queryOnSchemaMono) {
		return queryOnSchemaMono.map(queryOnSchema -> {
			return schema.execute(queryOnSchema.getCube(), queryOnSchema.getQuery(), queryOnSchema.getOptions());
		})
				.map(view -> ListBasedTabularView.load(view))
				.flatMap(view -> ServerResponse.ok()
						.contentType(MediaType.APPLICATION_JSON)
						.body(BodyInserters.fromValue(view)));
	}

	/**
	 * Execute an {@link eu.solven.adhoc.query.cube.IAdhocQuery} defined through POST parameter.
	 *
	 * @param serverRequest
	 * @return
	 */
	public Mono<ServerResponse> executeQuery(ServerRequest serverRequest) {
		Mono<TargetedAdhocQuery> queryOnSchemaMono = serverRequest.bodyToMono(TargetedAdhocQuery.class);
		return executeQuery(queryOnSchemaMono);
	}

	/**
	 * Execute an {@link eu.solven.adhoc.query.cube.IAdhocQuery} defined through GET parameters.
	 *
	 * @param serverRequest
	 * @return
	 */
	// cube=someCube&measure=k1.SUM&column_a=a1,a2&column_b>123&group_by=c
	public Mono<ServerResponse> executeFlatQuery(ServerRequest serverRequest) {
		TargetedAdhocQuery.TargetedAdhocQueryBuilder queryOnSchemaBuilder = TargetedAdhocQuery.builder();

		String cubeName = AdhocHandlerHelper.string(serverRequest, "cube_name");
		queryOnSchemaBuilder.cube(cubeName);

		AdhocQuery.AdhocQueryBuilder queryBuilder = AdhocQuery.builder();

		// https://stackoverflow.com/questions/24059773/correct-way-to-pass-multiple-values-for-same-parameter-name-in-get-request
		// `tag=a&tag=b` means we are looking for `a AND b`
		// `tag=a,b` means we are looking for `a OR b`
		// `tag=a,b&tag=c` means we are looking for `(a AND b) OR c`
		List<String> measures = serverRequest.queryParams().get("measure");
		if (measures != null) {
			queryBuilder.measures(measures);
		}
		List<String> groupBys = serverRequest.queryParams().get("group_by");
		if (measures != null) {
			groupBys.forEach(queryBuilder::groupByAlso);
		}

		{
			List<String> filteredColumns = serverRequest.queryParams()
					.keySet()
					.stream()
					.filter(k -> k.startsWith("column_"))
					.map(k -> k.substring("column_".length()))
					.distinct()
					.toList();

			// This simpler API do AND between input filteredColumns
			AndFilter.AndFilterBuilder andBuilder = AndFilter.builder();

			filteredColumns.forEach(filteredColumn -> {
				List<String> rawFilters = serverRequest.queryParams().get("column_" + filteredColumn);

				List<IValueMatcher> filters = rawFilters.stream()
						.distinct()
						.<IValueMatcher>map(rawFilter -> EqualsMatcher.builder().operand(rawFilter).build())
						.toList();

				ColumnFilter columnFilter = ColumnFilter.builder()
						.column(filteredColumn)
						.valueMatcher(OrMatcher.builder().operands(filters).build())
						.build();
				andBuilder.filter(columnFilter);
			});
			queryBuilder.filter(andBuilder.build());
		}
		queryOnSchemaBuilder.query(queryBuilder.build());

		return executeQuery(Mono.just(queryOnSchemaBuilder.build()));
	}
}