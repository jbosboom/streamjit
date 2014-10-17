/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
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
package edu.mit.streamjit.util;

import com.google.common.collect.AbstractIterator;
import java.util.Iterator;
import java.util.ServiceConfigurationError;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This iterator wraps an iterator and skips over any next() calls that complete
 * abruptly due to ServiceConfigurationError if the named provider does not
 * exist.
 *
 * This class exists because annotation processors supporting incremental
 * compilation are not able to remove a provider from META-INF/services when its
 * annotation is removed.  This iterator simulates their removal by skipping
 * those errors (but only those errors -- if the provider isn't instantiable,
 * for example, the error will still be thrown).
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 8/22/2013
 */
public class SkipMissingServicesIterator<T> extends AbstractIterator<T> {
	private static final Pattern NOT_FOUND_MESSAGE = Pattern.compile("Provider (.*) not found$");
	private final Iterator<? extends T> iterator;
	public SkipMissingServicesIterator(Iterator<? extends T> iterator) {
		this.iterator = iterator;
	}
	@Override
	protected T computeNext() {
		while (iterator.hasNext()) {
			try {
				return iterator.next();
			} catch (ServiceConfigurationError ex) {
				Matcher m = NOT_FOUND_MESSAGE.matcher(ex.getMessage());
				if (m.find())
					continue; //TODO: verify the provider doesn't exist? Do we trust ServiceLoader?
				throw ex;
			}
		}
		return endOfData();
	}
}
