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

import java.nio.file.FileSystems;

/**
 * Calls FSST through JNI.
 */
@SuppressWarnings({ "PMD", "checkstyle" })
class FsstJni implements Fsst {

	private static boolean isLoaded = false;
	private static UnsatisfiedLinkError loadError;

	static {
		String os = System.getProperty("os.name").toLowerCase();
		boolean isWindows = os.contains("win");
		String moduleDir = "build/FsstWrapper.so";
		if (isWindows) {
			// TODO: figure out how to get cmake to put in common directory
			moduleDir = "build/Release/FsstWrapper.so";
		}
		String modulePath = FileSystems.getDefault().getPath(moduleDir).normalize().toAbsolutePath().toString();
		try {
			System.load(modulePath);
			isLoaded = true;
		} catch (UnsatisfiedLinkError e) {
			loadError = e;
		}
	}

	public SymbolTable encode(byte[] data) {
		return FsstJni.compress(data);
	}

	public static boolean isLoaded() {
		return isLoaded;
	}

	public static UnsatisfiedLinkError getLoadError() {
		return loadError;
	}

	public static native SymbolTable compress(byte[] inputBytes);
}
