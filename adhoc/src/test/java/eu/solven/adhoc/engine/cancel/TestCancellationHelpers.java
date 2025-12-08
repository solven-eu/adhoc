package eu.solven.adhoc.engine.cancel;

import java.time.OffsetDateTime;

import org.junit.jupiter.api.Test;

import eu.solven.adhoc.engine.context.IIsCancellable;

public class TestCancellationHelpers {
	@Test
	public void testAfterCancelled() {
		IIsCancellable isCancellable = new IIsCancellable() {

			@Override
			public boolean isCancelled() {
				return true;
			}

			@Override
			public OffsetDateTime getCancellationDate() {
				return OffsetDateTime.now().minusSeconds(7);
			}

			@Override
			public void addCancellationListener(Runnable runnable) {

			}

			@Override
			public void removeCancellationListener(Runnable runnable) {

			}
		};
		CancellationHelpers.afterCancellable(isCancellable);
	}
}
