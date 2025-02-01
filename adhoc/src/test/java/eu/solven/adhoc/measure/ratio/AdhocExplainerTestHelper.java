package eu.solven.adhoc.measure.ratio;

import java.util.ArrayList;
import java.util.List;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import eu.solven.adhoc.dag.DAGExplainer;
import eu.solven.adhoc.eventbus.AdhocLogEvent;

/**
 * Useful to test {@link DAGExplainer}
 * 
 * @author Benoit Lacelle
 */
public class AdhocExplainerTestHelper {
	protected AdhocExplainerTestHelper() {
		// hidden
	}

	public static List<String> listenForExplain(EventBus eventBus) {
		List<String> messages = new ArrayList<>();

		// Register an eventListener to collect the EXPLAIN results
		{
			Object listener = new Object() {

				@Subscribe
				public void onExplainOrDebugEvent(AdhocLogEvent event) {
					if (event.isExplain()) {
						messages.add(event.getMessage());
					}
				}
			};

			eventBus.register(listener);
		}

		return messages;

	}

}
