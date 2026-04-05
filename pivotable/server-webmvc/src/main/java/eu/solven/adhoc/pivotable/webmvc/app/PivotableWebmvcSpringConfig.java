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
package eu.solven.adhoc.pivotable.webmvc.app;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import eu.solven.adhoc.pivotable.webmvc.PivotableWebmvcExceptionHandler;
import eu.solven.adhoc.pivotable.webmvc.api.CubesController;
import eu.solven.adhoc.pivotable.webmvc.api.PivotableClearController;
import eu.solven.adhoc.pivotable.webmvc.api.PivotableEndpointsController;
import eu.solven.adhoc.pivotable.webmvc.api.PivotableQueryController;
import eu.solven.adhoc.pivotable.webmvc.api.PivotableSpaController;
import eu.solven.adhoc.pivotable.webnone.PivotableWebnoneSpringConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * Wraps API-related components.
 * 
 * @author Benoit Lacelle
 */
@Import({

		// None
		PivotableWebnoneSpringConfig.class,

		// Webmvc
		PivotableSpaController.class,
		CubesController.class,
		PivotableEndpointsController.class,
		PivotableQueryController.class,
		PivotableClearController.class,
		PivotableWebmvcExceptionHandler.class,

})
@Slf4j
@Configuration
public class PivotableWebmvcSpringConfig {

}