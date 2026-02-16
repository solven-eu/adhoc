package eu.solven.adhoc.encoding.page;

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.encoding.column.freezer.AsynchronousFreezingStrategy;
import eu.solven.adhoc.encoding.column.freezer.SynchronousFreezingStrategy;
import eu.solven.adhoc.options.IHasQueryOptions;
import eu.solven.adhoc.options.StandardQueryOptions;

public class TestThreadLocalAppendableTableFactory {
	@Test
	public void testMake_synchronous() {
		ThreadLocalAppendableTableFactory factory = new ThreadLocalAppendableTableFactory();

		IAppendableTable table = factory.makeTable(IHasQueryOptions.noOption());

		Assertions.assertThat(table).isInstanceOfSatisfying(ThreadLocalAppendableTable.class, theTable -> {
			Assertions.assertThat(theTable.freezer).isInstanceOf(SynchronousFreezingStrategy.class);
		});
	}
	@Test
	public void testMake_asynchronous() {
		ThreadLocalAppendableTableFactory factory = new ThreadLocalAppendableTableFactory();

		IAppendableTable table = factory.makeTable(() -> Set.of(StandardQueryOptions.CONCURRENT));

		Assertions.assertThat(table).isInstanceOfSatisfying(ThreadLocalAppendableTable.class, theTable -> {
			Assertions.assertThat(theTable.freezer).isInstanceOf(AsynchronousFreezingStrategy.class);
		});
	}
}
