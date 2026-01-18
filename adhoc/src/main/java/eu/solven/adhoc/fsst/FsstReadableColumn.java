package eu.solven.adhoc.fsst;

import java.nio.charset.StandardCharsets;
import java.util.List;

import eu.solven.adhoc.compression.page.IReadableColumn;
import lombok.Builder;
import lombok.NonNull;

/**
 * 
 * @author Benoit Lacelle
 */
@Builder
public class FsstReadableColumn implements IReadableColumn {

	@NonNull
	protected FsstDecoder decoder;

	@NonNull
	protected List<byte[]> encoded;

	@Override
	public Object readValue(int rowIndex) {
		byte[] encodedBytes = encoded.get(rowIndex);

		if (encodedBytes == null) {
			return null;
		}

		byte[] decodedBytes = decoder.decode(encodedBytes);
		return new String(decodedBytes, StandardCharsets.UTF_8);
	}

}