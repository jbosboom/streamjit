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
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/7/2013
 */
public final class WeightedRoundrobinSplitter<T> extends Splitter<T> {
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
		return Collections.nCopies(outputs(), Rate.create(0));
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
