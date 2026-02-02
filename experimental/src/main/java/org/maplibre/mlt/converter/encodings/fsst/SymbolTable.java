/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.maplibre.mlt.converter.encodings.fsst;

import java.util.Arrays;

/**
 * Output of FSST-encoding: a symbol table, and text encoded with that symbol table.
 *
 * @param symbols
 *            Up to 255 symbols in the table concatenated together
 * @param symbolLengths
 *            Lengths of each of those 255 symbols in the table
 * @param compressedData
 *            Encoded text where a value less than 255 refers to that symbol from the table, and a value of 255 means
 *            the next byte should be used directly.
 */
@SuppressWarnings("checkstyle:all")public record SymbolTable(byte[]symbols,int[]symbolLengths,byte[]compressedData,int decompressedLength){@Override public String toString(){return"SymbolTable{weight="+weight()+", symbols=byte["+symbols.length+"], symbolLengths=int["+symbolLengths.length+"], compressedData=byte["+compressedData.length+"], decompressedLength="+decompressedLength+'}';}

public int weight(){return symbols.length+symbolLengths.length+compressedData.length;}

@Override public boolean equals(Object o){return this==o||(o instanceof SymbolTable that&&Arrays.equals(symbols,that.symbols)&&Arrays.equals(symbolLengths,that.symbolLengths)&&Arrays.equals(compressedData,that.compressedData));}

@Override public int hashCode(){int result=Arrays.hashCode(symbols);result=31*result+Arrays.hashCode(symbolLengths);result=31*result+Arrays.hashCode(compressedData);return result;}}
