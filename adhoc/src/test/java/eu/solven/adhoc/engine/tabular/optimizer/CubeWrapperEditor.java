package eu.solven.adhoc.engine.tabular.optimizer;

import java.util.Objects;

import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.engine.CubeQueryEngine;
import eu.solven.adhoc.engine.ICubeQueryEngine;
import eu.solven.adhoc.engine.tabular.ITableQueryEngine;
import eu.solven.adhoc.engine.tabular.TableQueryEngine;

/**
 * Helps modifying inner structures into a {@link ICubeWrapper}.
 * 
 * @author Benoit Lacelle
 */
public class CubeWrapperEditor {
	ICubeWrapper cubeWrapper;

	public CubeWrapperEditor(ICubeWrapper cubeWrapper) {
		Objects.requireNonNull(cubeWrapper, "cubeWrapper");
		this.cubeWrapper = cubeWrapper;
	}

	/**
	 * 
	 * @param optimizerFactory
	 * @return this builder after editing current {@link ICubeWrapper} with given {@link ITableQueryOptimizerFactory}.
	 */
	public CubeWrapperEditor editTableQueryOptimizer(ITableQueryOptimizerFactory optimizerFactory) {
		CubeWrapper rawCubeWrapper = (CubeWrapper) cubeWrapper;

		CubeQueryEngine rawCubeQueryEngine = (CubeQueryEngine) rawCubeWrapper.getEngine();
		ITableQueryEngine editedTableQueryEngine =
				((TableQueryEngine) rawCubeQueryEngine.getTableQueryEngine()).toBuilder()
						.optimizerFactory(optimizerFactory)
						.build();

		ICubeQueryEngine editedEngine = rawCubeQueryEngine.toBuilder().tableQueryEngine(editedTableQueryEngine).build();
		cubeWrapper = rawCubeWrapper.toBuilder().engine(editedEngine).build();

		return this;
	}

	public ICubeWrapper build() {
		return cubeWrapper;
	}
}
