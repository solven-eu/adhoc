package eu.solven.adhoc.dictionary.packing;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.base.Strings;

import eu.solven.adhoc.dictionary.IIntArray;
import lombok.Builder;
import lombok.NonNull;

/**
 * Used to compress an `int[]` when all ints are known to be relatively small. `bits` represent the number of expressed
 * bits: all others are considered to be 0.
 * 
 * @author Benoit Lacelle
 */
@Builder
public class PackedIntegers implements IIntArray {
	// number of lower bits expressed per integer (higher bits are all zeros)
	protected int bits;
	// number of packed ints
	protected int length;

	// each `byte` provide 8 bits.
	protected final int chunkSize = 32;
	// length is `(length * bits) / chunkSize`
	@NonNull
	protected int[] holder;

	@Override
	public int length() {
		return length;
	}

	@Override
	public void writeInt(int index, int value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int readInt(int index) {
		int output = 0;

		// maximum number of chunks amongst which the pack is dispatched
		// int nbChunks = 1 + (bits - 1) / chunkSize;

		int firstBitIndex = index * bits;

		// Index of the first chunk holding relevant bits
		int firstChunkIndex = firstBitIndex / chunkSize;
		// Number of bits to skip in the first chunk
		int firstChunkShift = firstBitIndex - firstChunkIndex * chunkSize;

		int bitsLeft = chunkSize;

		// We may need to skip some bits on the first chunk
		int shiftRead = firstChunkShift;
		// We need to append the writes
		int shiftWrite = 0;

		this.toString();

		while (bitsLeft > 0) {
			int mask = 0xFFFFFFFF >>> (32 - bits);

			int maskRead;
			if (shiftRead == 0) {
				maskRead = mask >>> shiftWrite;
			} else {
				maskRead = mask << shiftRead;
			}

			int contrib = holder[firstChunkIndex] & (maskRead);
			int contributionFromChunk;
			if (shiftRead == 0) {
				contributionFromChunk = contrib << shiftWrite;
			} else {
				contributionFromChunk = contrib >>> shiftRead;
			}
			output |= contributionFromChunk;

			firstChunkIndex++;
			int nbWritten = (chunkSize - shiftRead);
			bitsLeft -= nbWritten;
			shiftWrite += nbWritten;
			// Next chunk is read from the beginning
			shiftRead = 0;

		}

		return output;
	}

	@Override
	public String toString() {
		return IntStream.of(holder)
				.map(Integer::reverse)
				.mapToObj(i -> Strings.padStart(Integer.toBinaryString(i), 32, '0'))
				.collect(Collectors.joining("."));
	}
}
