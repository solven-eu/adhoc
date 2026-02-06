package eu.solven.adhoc.compression.page;

import eu.solven.adhoc.compression.column.AsynchronousFreezingStrategy;
import eu.solven.adhoc.compression.column.ObjectArrayColumnsFactory;
import eu.solven.adhoc.compression.column.SynchronousFreezingStrategy;
import eu.solven.adhoc.compression.column.freezer.AdhocFreezingUnsafe;
import eu.solven.adhoc.compression.column.freezer.IFreezingStrategy;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.IHasQueryOptions;
import eu.solven.adhoc.util.AdhocUnsafe;

/**
 * An {@link IAppendableTableFactory} based on {@link ThreadLocalAppendableTable}.
 *
 * @author Benoit Lacelle
 */
public class ThreadLocalAppendableTableFactory implements IAppendableTableFactory{
    @Override
    public IAppendableTable makeTable(IHasQueryOptions options) {
        IFreezingStrategy synchronousFreezer=
                SynchronousFreezingStrategy.builder().freezersWithContext(AdhocFreezingUnsafe.getFreezers()).build();

        IFreezingStrategy freezer;
        if (StandardQueryOptions.CONCURRENT.isActive(options.getOptions())) {
            freezer = AsynchronousFreezingStrategy.builder().synchronousStrategy(synchronousFreezer).build();
        } else {
            freezer = synchronousFreezer;
        }

        return ThreadLocalAppendableTable.builder().capacity(AdhocUnsafe.getPageSize())
                .columnsFactory(ObjectArrayColumnsFactory.builder() .freezer(freezer).build())
               .build();
    }
}
