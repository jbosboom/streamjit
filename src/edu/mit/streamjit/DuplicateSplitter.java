package edu.mit.streamjit;

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
	protected List<Rate> getPeekRates() {
		//We don't peek.
		return Collections.singletonList(Rate.create(0));
	}

	@Override
	protected List<Rate> getPopRates() {
		return Collections.singletonList(Rate.create(1));
	}

	@Override
	protected List<Rate> getPushRates() {
		return Collections.nCopies(outputs(), Rate.create(1));
	}
}
