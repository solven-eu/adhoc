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
package eu.solven.adhoc.table.sql;

import java.sql.Connection;
import java.util.List;

import javax.sql.DataSource;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.ExecuteListener;
import org.jooq.ExecuteListenerProvider;
import org.jooq.SQLDialect;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.tools.StopWatchListener;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.query.AdhocCaseSensitivity;
import lombok.Builder;
import lombok.RequiredArgsConstructor;

/**
 * Defines a {@link DSLSupplier} given a {@link Configuration}.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Builder
public class StandardDSLSupplier implements DSLSupplier {
	final Configuration configuration;

	@Override
	public DSLContext getDSLContext() {
		return DSL.using(configuration);
	}

	/**
	 * Lombok @Builder
	 */
	public static class StandardDSLSupplierBuilder {
		@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
		Configuration configuration = new DefaultConfiguration().set(AdhocCaseSensitivity.jooqSettings());

		public StandardDSLSupplierBuilder configuration(Configuration configuration) {
			this.configuration = configuration;

			return this;
		}

		public StandardDSLSupplierBuilder executeListeners(ExecuteListener... executeListeners) {
			configuration = configuration.set(executeListeners);

			return this;
		}

		public StandardDSLSupplierBuilder executeListenerProviders(
				ExecuteListenerProvider... executeListenerProviders) {
			configuration = configuration.set(executeListenerProviders);

			return this;
		}

		public StandardDSLSupplierBuilder settings(Settings settings) {
			configuration = configuration.set(settings);

			return this;
		}

		public StandardDSLSupplierBuilder dialect(SQLDialect sqlDialect) {
			configuration = configuration.set(sqlDialect);

			return this;
		}

		public StandardDSLSupplierBuilder dataSource(DataSource datasource) {
			configuration = configuration.set(datasource);

			return this;
		}

		public StandardDSLSupplierBuilder connection(Connection connection) {
			configuration = configuration.set(connection);

			return this;
		}

		@Deprecated(since = "Unsatble API")
		public StandardDSLSupplierBuilder stopWatch() {
			ExecuteListenerProvider[] existingProviders = configuration.executeListenerProviders();

			List<ExecuteListenerProvider> updatedProviders = ImmutableList.<ExecuteListenerProvider>builder()
					.addAll(List.of(existingProviders))
					.add(new ExecuteListenerProvider() {

						@Override
						public ExecuteListener provide() {
							return new StopWatchListener();
						}

					})
					.build();

			// https://www.jooq.org/doc/latest/manual/sql-execution/execute-listeners/
			// `ExecuteListenerProvider` enable having a fresh StopWatch per query
			return this.executeListenerProviders(updatedProviders.toArray(ExecuteListenerProvider[]::new));
		}
	}

}
