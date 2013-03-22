package edu.mit.streamjit.api;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;

/**
 * A RoundrobinJoiner joins its input by reading data items from each child in
 * turn per execution.  RoundrobinJoiner reads the same number of items per
 * child; to use weights, see WeightedRoundrobinJoiner.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/7/2012
 */
public final class RoundrobinJoiner<T> extends Joiner<T, T> {
	private final int itemsPerExecution;
	public RoundrobinJoiner() {
		this(1);
	}

	public RoundrobinJoiner(int itemsPerExecution) {
		this.itemsPerExecution = itemsPerExecution;
	}

	@Override
	public void work() {
		for (int i = 0; i < inputs(); ++i)
			for (int j = 0; j < itemsPerExecution; ++j)
				push(pop(i));
	}

	@Override
	public int supportedInputs() {
		return Joiner.UNLIMITED;
	}

	@Override
	public List<Rate> getPeekRates() {
		//We don't peek.
		return Collections.nCopies(inputs(), Rate.create(0));
	}

	@Override
	public List<Rate> getPopRates() {
		return Collections.nCopies(inputs(), Rate.create(itemsPerExecution));
	}

	@Override
	public ImmutableList<Rate> getPushRates() {
		return ImmutableList.of(Rate.create(itemsPerExecution*inputs()));
	}

	@Override
	public String toString() {
		return String.format("RoundrobinJoiner(%d)", itemsPerExecution);
	}
}
