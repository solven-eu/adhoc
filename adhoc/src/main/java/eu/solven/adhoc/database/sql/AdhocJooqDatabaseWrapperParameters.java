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
package eu.solven.adhoc.database.sql;

import org.jooq.Name;
import org.jooq.TableLike;
import org.jooq.impl.DSL;

import eu.solven.adhoc.database.transcoder.IAdhocDatabaseTranscoder;
import eu.solven.adhoc.database.transcoder.IdentityTranscoder;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
public class AdhocJooqDatabaseWrapperParameters {

	@Builder.Default
	@NonNull
	@Getter
	final IAdhocDatabaseTranscoder transcoder = new IdentityTranscoder();

	@NonNull
	DSLSupplier dslSupplier;

	@NonNull
	final TableLike<?> table;

	public static class AdhocJooqDatabaseWrapperParametersBuilder {
		public AdhocJooqDatabaseWrapperParametersBuilder tableName(String tableName) {
			this.tableName(DSL.quotedName(tableName));

			return this;
		}

		public AdhocJooqDatabaseWrapperParametersBuilder tableName(Name tableName) {
			this.table(DSL.table(tableName));

			return this;
		}
	}
}
