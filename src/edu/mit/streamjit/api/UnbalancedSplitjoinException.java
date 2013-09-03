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
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
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
