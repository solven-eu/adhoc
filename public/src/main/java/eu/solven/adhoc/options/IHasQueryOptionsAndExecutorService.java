package eu.solven.adhoc.options;

import java.util.Set;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Relevant as we often check for {@link StandardQueryOptions#CONCURRENT}, in which case we would need the relevant
 * {@link ListeningExecutorService} to submit tasks.
 * 
 * @author Benoit Lacelle
 */
public interface IHasQueryOptionsAndExecutorService extends IHasQueryOptions {
	ListeningExecutorService getExecutorService();

	static IHasQueryOptionsAndExecutorService noOption() {
		IHasQueryOptions options = IHasQueryOptions.noOption();
		ListeningExecutorService les = MoreExecutors.newDirectExecutorService();

		return new IHasQueryOptionsAndExecutorService() {

			@Override
			public Set<IQueryOption> getOptions() {
				return options.getOptions();
			}

			@Override
			public ListeningExecutorService getExecutorService() {
				return les;
			}
		};
	}
}
