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
package eu.solven.adhoc.engine.tabular.optimizer;

import java.util.Objects;

import eu.solven.adhoc.column.ColumnsManager;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.engine.CubeQueryEngine;
import eu.solven.adhoc.engine.ICubeQueryEngine;
import eu.solven.adhoc.engine.tabular.ITableQueryEngine;
import eu.solven.adhoc.engine.tabular.TableQueryEngine;
import eu.solven.adhoc.table.transcoder.ITableAliaser;

/**
 * Helps modifying inner structures into a {@link ICubeWrapper}.
 * 
 * @author Benoit Lacelle
 */
public class CubeWrapperEditor {
	ICubeWrapper cubeWrapper;

	private CubeWrapperEditor(ICubeWrapper cubeWrapper) {
		Objects.requireNonNull(cubeWrapper, "cubeWrapper");
		this.cubeWrapper = cubeWrapper;
	}

	public static CubeWrapperEditor edit(ICubeWrapper cube) {
		return new CubeWrapperEditor(cube);
	}

	/**
	 * 
	 * @param optimizerFactory
	 * @return this builder after editing current {@link ICubeWrapper} with given {@link ITableQueryOptimizerFactory}.
	 */
	public CubeWrapperEditor editTableQueryOptimizer(ITableQueryOptimizerFactory optimizerFactory) {
		CubeWrapper rawCubeWrapper = (CubeWrapper) cubeWrapper;

		CubeQueryEngine rawCubeQueryEngine = (CubeQueryEngine) rawCubeWrapper.getEngine();
		TableQueryEngine rawTableQueryEngine = (TableQueryEngine) rawCubeQueryEngine.getTableQueryEngine();
		ITableQueryEngine editedTableQueryEngine =
				rawTableQueryEngine.toBuilder().optimizerFactory(optimizerFactory).build();

		ICubeQueryEngine editedEngine = rawCubeQueryEngine.toBuilder().tableQueryEngine(editedTableQueryEngine).build();
		cubeWrapper = rawCubeWrapper.toBuilder().engine(editedEngine).build();

		return this;
	}

	public ICubeWrapper build() {
		return cubeWrapper;
	}

	public CubeWrapperEditor aliaser(ITableAliaser aliaser) {
		CubeWrapper rawCubeWrapper = (CubeWrapper) cubeWrapper;

		ColumnsManager rawColumnsManager = (ColumnsManager) rawCubeWrapper.getColumnsManager();
		ColumnsManager newColumnsManager = rawColumnsManager.toBuilder().aliaser(aliaser).build();

		cubeWrapper = rawCubeWrapper.toBuilder().columnsManager(newColumnsManager).build();

		return this;
	}
}
