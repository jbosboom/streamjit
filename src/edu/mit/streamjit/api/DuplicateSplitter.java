package edu.mit.streamjit.api;

import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.Rate;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
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
	public List<Rate> getPeekRates() {
		//We don't peek.
		return Collections.singletonList(Rate.create(0));
	}

	@Override
	public List<Rate> getPopRates() {
		return Collections.singletonList(Rate.create(1));
	}

	@Override
	public List<Rate> getPushRates() {
		return Collections.nCopies(outputs(), Rate.create(1));
	}
}
