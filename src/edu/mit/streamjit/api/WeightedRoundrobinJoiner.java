package edu.mit.streamjit.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A WeightedRoundrobinJoiner joins its input by taking data items from its
 * children according to specified weights.  A WeightedRoundrobinJoiner with
 * weights [1, 2, 1] will take one item from its first parent, two items from
 * its second parent, and one item from its third parent per iteration.
 *
 * TODO: see WeightedRoundrobinSplitter for details about this class' necessity
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/7/2012
 */
public final class WeightedRoundrobinJoiner<T> extends Joiner<T, T> {
	private final int[] weights;
	public WeightedRoundrobinJoiner(int... weights) {
		this.weights = weights;
	}

	@Override
	public void work() {
		for (int i = 0; i < inputs(); ++i)
			for (int j = 0; j < weights[i]; ++j)
				push(pop(i));
	}

	@Override
	public int supportedInputs() {
		return weights.length;
	}

	@Override
	public List<Rate> getPeekRates() {
		//We don't peek.
		return Collections.nCopies(inputs(), Rate.create(0));
	}

	@Override
	public List<Rate> getPopRates() {
		List<Rate> r = new ArrayList<>();
		for (int w : weights)
			r.add(Rate.create(w));
		return Collections.unmodifiableList(r);
	}

	@Override
	public List<Rate> getPushRates() {
		int sum = 0;
		for (int w : weights)
			sum += w;
		return Collections.singletonList(Rate.create(sum));
	}

	@Override
	public String toString() {
		return String.format("WeightedRoundrobinJoiner(%s)", Arrays.toString(weights));
	}
}
