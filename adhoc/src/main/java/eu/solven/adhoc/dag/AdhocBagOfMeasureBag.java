package eu.solven.adhoc.dag;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class AdhocBagOfMeasureBag {
	final Map<String, AdhocMeasureBag> nameToMeasureBag = new TreeMap<>();

	public int size() {
		return nameToMeasureBag.size();
	}

	public AdhocMeasureBag getBag(String name) {
		return nameToMeasureBag.get(name);
	}

	public AdhocBagOfMeasureBag putBag(String name, AdhocMeasureBag bag) {
		nameToMeasureBag.put(name, bag);

		return this;
	}

	public Set<String> bagNames() {
		return nameToMeasureBag.keySet();
	}
}
