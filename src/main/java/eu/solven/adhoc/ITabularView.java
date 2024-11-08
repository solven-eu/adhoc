package eu.solven.adhoc;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Storage for static data (i.e. not Live data).
 * 
 * @author Benoit Lacelle
 *
 */
public interface ITabularView {
	Stream<Map<String, ?>> keySet();

	void acceptScanner(RowScanner rowScanner);
}
