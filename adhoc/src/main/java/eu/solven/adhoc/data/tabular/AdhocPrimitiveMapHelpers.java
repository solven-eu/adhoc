package eu.solven.adhoc.data.tabular;

import java.util.Collection;

import eu.solven.adhoc.util.AdhocUnsafe;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import lombok.experimental.UtilityClass;

/**
 * 
 * Helps building {@link Collection}, especially if it relies on a primitive type.
 * 
 * @author Benoit Lacelle
 * 
 */
@UtilityClass
public class AdhocPrimitiveMapHelpers {

	@SuppressWarnings("PMD.LooseCoupling")
	public static <T> Object2IntMap<T> newHashMapDefaultMinus1() {
		return newHashMapDefaultMinus1(AdhocUnsafe.getDefaultColumnCapacity());
	}

	public static <T> Object2IntMap<T> newHashMapDefaultMinus1(int size) {
		Object2IntOpenHashMap<T> map = new Object2IntOpenHashMap<>(size);

		// If we request an unknown slice, we must not map to an existing index
		map.defaultReturnValue(-1);

		return map;
	}

}
