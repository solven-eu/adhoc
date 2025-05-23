package eu.solven.adhoc.table.transcoder;

import com.google.common.collect.ImmutableList;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;

/**
 * Helps combining multiple {@link ITableTranscoder}.
 * 
 * @author Benoit Lacelle
 */
@Builder
public class CompositeTableTranscoder implements ITableTranscoder {
	public enum ChainMode {
		/**
		 * Should we return the first not-null underlying
		 */
		FirstNotNull,

		/**
		 * All transcoders, as a chain of transcoders
		 */
		ApplyAll
	}

	@NonNull
	@Singular
	ImmutableList<ITableTranscoder> transcoders;

	@Default
	@NonNull
	ChainMode chainMode = ChainMode.FirstNotNull;

	@Override
	public String underlying(String queried) {
		boolean oneMatched = false;
		String currenQueried = queried;

		for (ITableTranscoder transcoder : transcoders) {
			String underlying = transcoder.underlying(currenQueried);

			if (underlying != null) {
				oneMatched = true;

				if (chainMode == ChainMode.FirstNotNull) {
					return underlying;
				} else {
					currenQueried = underlying;
				}
			}
		}

		// null means `not transcoded`
		if (oneMatched) {
			return currenQueried;
		} else {
			return null;
		}
	}

}
