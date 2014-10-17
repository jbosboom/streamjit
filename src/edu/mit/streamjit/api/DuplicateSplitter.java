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
package edu.mit.streamjit.api;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;

/**
 * DuplicateSplitter splits its input by duplicating input data items to each
 * of its outputs.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 11/7/2012
 */
public final class DuplicateSplitter<T> extends Splitter<T, T>{
	public DuplicateSplitter() {
	}

	@Override
	public void work() {
		T item = pop();
		for (int i = 0; i < outputs(); ++i)
			push(i, item);
	}

	@Override
	public int supportedOutputs() {
		return Splitter.UNLIMITED;
	}

	@Override
	public ImmutableList<Rate> getPeekRates() {
		//We don't peek.
		return ImmutableList.of(Rate.create(0));
	}

	@Override
	public ImmutableList<Rate> getPopRates() {
		return ImmutableList.of(Rate.create(1));
	}

	@Override
	public List<Rate> getPushRates() {
		return Collections.nCopies(outputs(), Rate.create(1));
	}
}
