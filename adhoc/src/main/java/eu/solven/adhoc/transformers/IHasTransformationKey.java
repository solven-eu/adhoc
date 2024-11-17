package eu.solven.adhoc.transformers;

import java.util.Map;

public interface IHasTransformationKey {

	String getTransformationKey();

	Map<String, ?> getTransformationOptions();

}
