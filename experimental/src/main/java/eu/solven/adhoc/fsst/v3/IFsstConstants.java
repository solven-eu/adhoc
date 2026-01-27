package eu.solven.adhoc.fsst.v3;

// Core constants for FSST compression algorithm
public interface IFsstConstants {

	// https://github.com/cwida/fsst/blob/master/libfsst.hpp#L61
	int fsstLenBits = 12;
	int fsstCodeBits = 9;
	/* first 256 codes [0,255] are pseudo codes: escaped bytes */
	int fsstCodeBase = 256;
	/* all bits set: indicating a symbol that has not been assigned a code yet */
	int fsstCodeMax = 1 << fsstCodeBits; // 512
	int fsstCodeMask = fsstCodeMax - 1; // 0x1FF

	// https://github.com/cwida/fsst/blob/master/libfsst.hpp#L118
	int fsstHashLog2Size = 10;
	// // smallest size that incurs no precision loss
	int fsstHashTabSize = 1 << fsstHashLog2Size; // 2048 entries
	// https://github.com/cwida/fsst/blob/master/libfsst.hpp#L119
	long fsstHashPrime = 2971215073L; // Prime for multiplicative hashing
	// https://github.com/cwida/fsst/blob/master/libfsst.hpp#L120
	int fsstShift = 15;

	// fsstICLFree marks unused hash table slots.
	// Layout: length=15 (impossible) at bits 28-31, code=0x1FF at bits 16-27
	// https://github.com/cwida/fsst/blob/master/libfsst.hpp#L159
	// eu.solven.adhoc.fsst.v3.SymbolUtil.Symbol.evalICL(int, int)
	long fsstICLFree = ((long) 15 << 28) | ((long) fsstCodeMask << 16);

	int fsstEscapeCode = 255; // Code 255 indicates next byte is literal
	int fsstMaxSymbols = 255; // Maximum number of learned symbols (codes 0-254)
	int fsstChunkSize = 511; // Process input in 511-byte chunks for cache efficiency

	// Training subsampling mask (0-127 range for deterministic sampling)
	int fsstSampleMask = 127;

	// Buffer padding for safe unaligned 8-byte loads at chunk boundaries
	int fsstChunkPadding = 9; // 511+9=520: allows 8-byte load at position 511 (511+8-1=518 < 520)

	// Output buffer growth factor for worst-case expansion
	// Worst case: every byte escapes (2 bytes per input byte) + small safety margin
	int fsstOutputPadding = 7; // Safety margin for edge cases

	// Bit masks for symbol operations
	int fsstMask8 = 0xFF; // 8-bit mask (1 byte)
	int fsstMask16 = 0xFFFF; // 16-bit mask (2 bytes)
	int fsstMask24 = 0xFFFFFF; // 24-bit mask (3 bytes)

	// --- Constants ---
	public static final long FSST_VERSION = 20190218L;

}
