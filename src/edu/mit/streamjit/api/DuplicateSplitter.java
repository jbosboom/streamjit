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
