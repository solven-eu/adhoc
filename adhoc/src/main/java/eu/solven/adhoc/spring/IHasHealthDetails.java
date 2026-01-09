package eu.solven.adhoc.spring;

import java.util.Map;

/**
 * Useful to optionally provide additional health information.
 * 
 * @author Benoit Lacelle
 */
public interface IHasHealthDetails {

	Map<String, ?> getHealthDetails();

}
