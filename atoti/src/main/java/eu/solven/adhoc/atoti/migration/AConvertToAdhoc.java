package eu.solven.adhoc.atoti.migration;

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
 * Helps processing an {@link IActivePivotManagerDescription} to generate a batch of {@link eu.solven.adhoc.measure.IMeasureForest}.
 *
 * This would run in a JDK21 jvm, while ActivePivot is generally expected to run over JDK11. One may encounter some issues:
 *
 * -
 *
 * - com.quartetfs.fwk.types.impl.TransferCompiler may need to call `javassist.CtClass#toClass(java.lang.Class)` (https://github.com/jboss-javassist/javassist/issues/369)
 */
@SuperBuilder
public abstract class AConvertToAdhoc {
    @NonNull
    IActivePivotManagerDescription apManagerDescription;


    @NonNull
    @Builder.Default
    AtotiMeasureToAdhoc apToAdhoc = AtotiMeasureToAdhoc.builder().build();

    /**
     * Helps loading ActivePivot configuration over a JDK21.
     *
     * As this is a static mutating method, it should be done at the same time {@link com.quartetfs.fwk.impl.QFSRegistry} is initialized, and SLF4JBridgeHandler is installed.
     */
    public static void enableActivePivot5OnJDK17() {
        // make sure that JUL logs are all redirected to SLF4J
//            SLF4JBridgeHandler.removeHandlersForRootLogger();
//            SLF4JBridgeHandler.install();

        // This will help Adhoc producing nicer schema, depending on PP properties
//            ApplicationConfig.resetQfsStaticThings();

        // Trying to workaround the need for JDK11 `java.nio.Bits.reserveMemory`
        System.setProperty(ActiveViamProperty.DEFAULT_CHUNK_SIZE_PROPERTY.getKey(), "1024");
        System.setProperty(ActiveViamProperty.CHUNK_ALLOCATOR_CLASS_PROPERTY.getKey(), HeapBufferChunkAllocator.class.getName());
    }

    public void convert() {
        MeasureForests.MeasureForestsBuilder forests = MeasureForests.builder();

        // Local cubes
        apManagerDescription.getSchemas().stream().parallel().flatMap(s -> s.getActivePivotSchemaDescription().getActivePivotInstanceDescriptions().stream()).forEach(apDesc -> {
            IActivePivotDescription apDescription = apDesc.getActivePivotDescription();
            String pivotId = apDesc.getId();
            forests.forest(fromActivePivotToAdhoc(pivotId, apDescription));
        });

        // DistributedCubes
        apManagerDescription.getSchemas().stream().parallel().flatMap(s -> s.getActivePivotSchemaDescription().getDistributedActivePivotInstanceDescriptions().stream()).forEach(apDesc -> {
            IActivePivotDescription apDescription = apDesc.getDistributedActivePivotDescription();
            String pivotId = apDesc.getId();
            forests.forest(fromActivePivotToAdhoc(pivotId, apDescription));
        });

        onForests(forests.build());
    }

    public IMeasureForest fromActivePivotToAdhoc(String pivotId, IActivePivotDescription apDescription) {
        return apToAdhoc.asForest(pivotId, apDescription);
    }

    protected abstract void onForests(MeasureForests forests) ;
}
