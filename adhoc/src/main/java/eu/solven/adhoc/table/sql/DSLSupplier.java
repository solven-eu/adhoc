/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
import java.util.function.Supplier;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import lombok.NonNull;

/**
 * Helps building a proper {@link DSLContext}, typically to {@link JooqTableWrapper}.
 * 
 * @author Benoit Lacelle
 *
 */
public interface DSLSupplier {
	DSLContext getDSLContext();

	/**
	 *
	 * @param connectionSupplier
	 * @return a {@link DSLSupplier} based on provided connectionSupplier
	 */
	static DSLSupplier fromConnection(Supplier<Connection> connectionSupplier) {
		// TODO Should rely on Connection provider
		return () -> DSL.using(connectionSupplier.get());
	}

	/**
	 *
	 * @param sqlDialect
	 * @return a {@link DSLSupplier} based on provided {@link SQLDialect}
	 */
	static @NonNull DSLSupplier fromDialect(SQLDialect sqlDialect) {
		return () -> DSL.using(sqlDialect);
	}
}
