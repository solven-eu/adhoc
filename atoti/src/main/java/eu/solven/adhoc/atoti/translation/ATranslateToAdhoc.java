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
package eu.solven.adhoc.atoti.translation;

import com.activeviam.properties.impl.ActiveViamProperty;
import com.qfs.chunk.buffer.allocator.impl.HeapBufferChunkAllocator;
import com.quartetfs.biz.pivot.definitions.IActivePivotDescription;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;

import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.resource.MeasureForests;
import lombok.Builder;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

/**
 * Helps processing an {@link IActivePivotManagerDescription} to generate a batch of
 * {@link eu.solven.adhoc.measure.IMeasureForest}.
 *
 * This would run in a JDK21 jvm, while ActivePivot is generally expected to run over JDK11. One may encounter some
 * issues:
 *
 * - com.quartetfs.fwk.types.impl.TransferCompiler may need to call `javassist.CtClass#toClass(java.lang.Class)`
 * (https://github.com/jboss-javassist/javassist/issues/369)
 * 
 * @author Benoit Lacelle
 */
@SuperBuilder
public abstract class ATranslateToAdhoc {
	@NonNull
	IActivePivotManagerDescription apManagerDescription;

	@NonNull
	@Builder.Default
	AtotiMeasureToAdhoc apToAdhoc = AtotiMeasureToAdhoc.builder().build();

	/**
	 * Helps loading ActivePivot configuration over a JDK21.
	 *
	 * As this is a static mutating method, it should be done at the same time
	 * {@link com.quartetfs.fwk.impl.QFSRegistry} is initialized, and SLF4JBridgeHandler is installed.
	 */
	public static void enableActivePivot5OnJDK17() {
		// make sure that JUL logs are all redirected to SLF4J
		// SLF4JBridgeHandler.removeHandlersForRootLogger();
		// SLF4JBridgeHandler.install();

		// This will help Adhoc producing nicer schema, depending on PP properties
		// ApplicationConfig.resetQfsStaticThings();

		// Trying to workaround the need for JDK11 `java.nio.Bits.reserveMemory`
		System.setProperty(ActiveViamProperty.DEFAULT_CHUNK_SIZE_PROPERTY.getKey(), "1024");
		System.setProperty(ActiveViamProperty.CHUNK_ALLOCATOR_CLASS_PROPERTY.getKey(),
				HeapBufferChunkAllocator.class.getName());
	}

	public void translate() {
		MeasureForests.MeasureForestsBuilder forests = MeasureForests.builder();

		// Local cubes
		apManagerDescription.getSchemas()
				.stream()
				.parallel()
				.flatMap(s -> s.getActivePivotSchemaDescription().getActivePivotInstanceDescriptions().stream())
				.forEach(apDesc -> {
					IActivePivotDescription apDescription = apDesc.getActivePivotDescription();
					String pivotId = apDesc.getId();
					forests.forest(fromActivePivotToAdhoc(pivotId, apDescription));
				});

		// DistributedCubes
		apManagerDescription.getSchemas()
				.stream()
				.parallel()
				.flatMap(s -> s.getActivePivotSchemaDescription()
						.getDistributedActivePivotInstanceDescriptions()
						.stream())
				.forEach(apDesc -> {
					IActivePivotDescription apDescription = apDesc.getDistributedActivePivotDescription();
					String pivotId = apDesc.getId();
					forests.forest(fromActivePivotToAdhoc(pivotId, apDescription));
				});

		onForests(forests.build());
	}

	public IMeasureForest fromActivePivotToAdhoc(String pivotId, IActivePivotDescription apDescription) {
		return apToAdhoc.asForest(pivotId, apDescription);
	}

	protected abstract void onForests(MeasureForests forests);
}
