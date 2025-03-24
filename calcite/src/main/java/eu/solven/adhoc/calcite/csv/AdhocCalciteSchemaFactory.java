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

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import com.google.common.eventbus.EventBus;

import eu.solven.adhoc.cube.AdhocCubeWrapper;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.table.IAdhocTableWrapper;
import eu.solven.pepper.mappath.MapPathGet;

/**
 * Factory that creates a {@link AdhocCalciteSchema}.
 *
 * <p>
 * Allows a custom schema to be included in a <code><i>model</i>.json</code> file.
 */
public class AdhocCalciteSchemaFactory implements SchemaFactory {
	/** Public singleton, per factory contract. */
	public static final AdhocCalciteSchemaFactory INSTANCE = new AdhocCalciteSchemaFactory();

	final EventBus eventBus;

	// Public default constructor required to instantiate a factory from
	public AdhocCalciteSchemaFactory() {
		this.eventBus = new EventBus();
	}

	@Override
	public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
		MeasureForest amb = MeasureForest.builder().build();
		AdhocQueryEngine aqe = AdhocQueryEngine.builder().eventBus(eventBus::post).build();

		IAdhocTableWrapper adw = makeTableWrapper(operand);

		AdhocCubeWrapper aqw = AdhocCubeWrapper.builder().engine(aqe).forest(amb).table(adw).build();

		return new AdhocCalciteSchema(aqw);
	}

	private IAdhocTableWrapper makeTableWrapper(Map<String, ?> operand) {
		String dbWrapperClass = MapPathGet.getRequiredString(operand, "tableWrapperFactoryClass");

		Class<?> clazz;
		try {
			clazz = (Class<?>) Class.forName(dbWrapperClass);
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException("Issue with class=" + dbWrapperClass, e);
		}

		try {
			return (IAdhocTableWrapper) clazz.getMethod("newInstance", Map.class).invoke(null, operand);
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
				| SecurityException e) {
			throw new IllegalArgumentException("Issue with class=" + dbWrapperClass, e);
		}
	}
}
