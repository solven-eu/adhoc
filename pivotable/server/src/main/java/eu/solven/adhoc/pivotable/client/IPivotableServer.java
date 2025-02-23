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
package eu.solven.adhoc.pivotable.client;

import eu.solven.adhoc.beta.schema.QueryOnSchema;
import eu.solven.adhoc.beta.schema.SchemaMetadata;
import eu.solven.adhoc.pivotable.entrypoint.AdhocEntrypointMetadata;
import eu.solven.adhoc.pivotable.entrypoint.AdhocEntrypointSearch;
import eu.solven.adhoc.storage.ITabularView;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Wraps Pivotable API
 * 
 * @author Benoit Lacelle
 *
 */
public interface IPivotableServer {
	Flux<AdhocEntrypointMetadata> searchEntrypoints(AdhocEntrypointSearch search);

	Flux<SchemaMetadata> searchSchemas(AdhocEntrypointSearch search);

	Mono<ITabularView> executeQuery(QueryOnSchema query);

}
