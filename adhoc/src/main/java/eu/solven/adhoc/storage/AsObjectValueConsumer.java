/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.storage;

import java.util.function.Consumer;

import lombok.Value;

/**
 * Simple {@link IValueConsumer} treating all inputs as Objects
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Deprecated(since = "IValueConsumer is a functional interface")
public class AsObjectValueConsumer implements IValueConsumer {

	final Consumer<Object> consumer;

	@Override
	public void onDouble(double d) {
		consumer.accept(d);
	}

	@Override
	public void onLong(long l) {
		consumer.accept(l);
	}

	@Override
	public void onCharsequence(CharSequence charSequence) {
		// Turn to String for simplification
		// If performance is a matter, do not user AsObjectValueConsumer at all
		consumer.accept(charSequence.toString());
	}

	@Override
	public void onObject(Object object) {
		consumer.accept(object);
	}

	public static AsObjectValueConsumer consumer(Consumer<Object> consumer) {
		return new AsObjectValueConsumer(consumer);
	}
}
