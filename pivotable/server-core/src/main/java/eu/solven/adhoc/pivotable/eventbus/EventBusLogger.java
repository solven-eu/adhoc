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
package eu.solven.adhoc.pivotable.eventbus;

import java.util.logging.Level;

import org.greenrobot.eventbus.EventBus;
import org.greenrobot.eventbus.Logger;
import org.greenrobot.eventbus.NoSubscriberEvent;
import org.greenrobot.eventbus.Subscribe;

import lombok.extern.slf4j.Slf4j;

/**
 * Transcode from JUL to SLF4J for {@link EventBus}
 * 
 * @author Benoit Lacelle
 *
 */
@Slf4j
public class EventBusLogger implements Logger {

	@Override
	public void log(Level level, String msg) {
		if (level == Level.SEVERE) {
			log.error("{}", msg);
		} else if (level == Level.WARNING) {
			log.warn("{}", msg);
		} else if (level == Level.INFO) {
			log.info("{}", msg);
		} else if (level == Level.FINE) {
			log.debug("{}", msg);
		} else if (level == Level.FINER || level == Level.FINEST) {
			log.trace("{}", msg);
		} else {
			log.error("Unmanaged level={}. Original message: {}", level, msg);
		}
	}

	@Override
	public void log(Level level, String msg, Throwable t) {
		if (level == Level.SEVERE) {
			log.error("{}", msg, t);
		} else if (level == Level.WARNING) {
			log.warn("{}", msg, t);
		} else if (level == Level.INFO) {
			log.info("{}", msg, t);
		} else if (level == Level.FINE) {
			log.debug("{}", msg, t);
		} else if (level == Level.FINER || level == Level.FINEST) {
			log.trace("{}", msg, t);
		} else {
			log.error("Unmanaged level={}. Original message: {}", level, msg, t);
		}
	}

	@Subscribe
	public void onNoSubscriberEvent(NoSubscriberEvent noSubscriberEvent) {
		log.warn("No subscriberEvent for {}", noSubscriberEvent.originalEvent);
	}
}
