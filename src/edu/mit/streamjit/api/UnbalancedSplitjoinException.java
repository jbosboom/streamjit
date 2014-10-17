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
import com.google.common.collect.Range;
import edu.mit.streamjit.util.Fraction;
import java.util.List;

/**
 * Thrown when a splitjoin's branches do not have compatible overall rates,
 * leading to either infinite buffering or deadlock when executed.
 * <p/>
 * Rates are represented as Range<Fraction>s, where the lower bound is the
 * minimum number of output items per input item, and the upper bound is the
 * maximum number of output items per input item. If the branches have static
 * rates, the minimum and maximum will be the same.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 9/2/2013
 */
public class UnbalancedSplitjoinException extends IllegalStreamGraphException {
	private static final long serialVersionUID = 1L;
	private final ImmutableList<Range<Fraction>> branchRates;
	public UnbalancedSplitjoinException(Splitjoin<?, ?> splitjoin, List<Range<Fraction>> branchRates) {
		super(String.valueOf(branchRates), splitjoin);
		this.branchRates = ImmutableList.copyOf(branchRates);
	}
	public List<Range<Fraction>> getBranchRates() {
		return branchRates;
	}
}
