package edu.mit.streamjit.api;

import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.Rate;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/7/2012
 */
public class RoundrobinJoiner<T> extends Joiner<T, T> {
	public RoundrobinJoiner() {
	}

	@Override
	public void work() {
		for (int i = 0; i < inputs(); ++i)
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
		return Collections.nCopies(inputs(), Rate.create(1));
	}

	@Override
	public List<Rate> getPushRates() {
		return Collections.singletonList(Rate.create(inputs()));
	}
}
