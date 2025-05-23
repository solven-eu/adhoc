/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.atoti.measure;

import java.util.Map;

import com.quartetfs.biz.pivot.postprocessing.impl.ArithmeticFormulaPostProcessor;

import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.operator.StandardOperatorsFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Extends {@link StandardOperatorsFactory} with Atoti additional operators.
 */
@Slf4j
@RequiredArgsConstructor
public class AtotiOperatorsFactory extends StandardOperatorsFactory {

	@Override
    public ICombination makeCombination(String key, Map<String, ?> options) {
        return switch (key) {
            case ArithmeticFormulaPostProcessor.PLUGIN_KEY: {
                yield new ArithmeticFormulaCombination(options);
            }
            default:
                yield super.makeCombination(key, options);
        };
    }
}
