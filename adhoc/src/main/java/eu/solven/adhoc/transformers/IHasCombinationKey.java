package eu.solven.adhoc.transformers;

import java.util.Map;

public interface IHasCombinationKey {

	String getCombinationKey();

	Map<String, ?> getCombinationOptions();

}
