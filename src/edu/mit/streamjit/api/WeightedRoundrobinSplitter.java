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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A WeightedRoundrobinSpliiter splits its input by passing data items to its
 * children according to specified weights.  A WeightedRoundrobinSpliiter with
 * weights [1, 2, 1] will pass one item to its first child, two items to its
 * second child, and one item to its third child per iteration.
 *
 * TODO: This class is separate from RoundrobinSplitter to avoid having to
 * branch in the work() function to determine the inner loop bound (which would
 * create extra work for the compiler to optimize away). The obvious solution,
 * using a weights array with identical weights, will not work because the
 * splitter doesn't learn how many outputs it has before they're hooked up, and
 * there's no notification between having all outputs hooked up and the first
 * execution. When the compiler is sophisticated enough to constant-fold the
 * branch, we could merge this class with RoundrobinSplitter. (The interpreter
 * would still take the branch, but its performance isn't critical.) The
 * preceeding comments apply to WeightedRoundrobinJoiner as well.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/7/2013
 */
public final class WeightedRoundrobinSplitter<T> extends Splitter<T, T> {
	private final int[] weights;
	/**
	 * Creates a new WeightedRoundrobinSplitter with the given weights.
	 */
	public WeightedRoundrobinSplitter(int... weights) {
		this.weights = weights;
	}

	@Override
	public int supportedOutputs() {
		return weights.length;
	}

	@Override
	public void work() {
		for (int i = 0; i < outputs(); ++i)
			for (int j = 0; j < weights[i]; ++j)
				push(i, pop());
	}

	@Override
	public List<Rate> getPeekRates() {
		//We don't peek.
		return ImmutableList.of(Rate.create(0));
	}

	@Override
	public ImmutableList<Rate> getPopRates() {
		int sum = 0;
		for (int w : weights)
			sum += w;
		return ImmutableList.of(Rate.create(sum));
	}

	@Override
	public ImmutableList<Rate> getPushRates() {
		ImmutableList.Builder<Rate> r = ImmutableList.builder();
		for (int w : weights)
			r.add(Rate.create(w));
		return r.build();
	}

	@Override
	public String toString() {
		return String.format("WeightedRoundrobinSplitter(%s)", Arrays.toString(weights));
	}
}
