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
package edu.mit.streamjit.impl.compiler2;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.collect.Range;
import com.google.common.math.IntMath;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import java.math.RoundingMode;
import java.util.List;
import java.util.Set;

/**
 * An allocation strategy that fully data-parallelizes over up to N cores.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 12/31/2013
 */
public final class FullDataParallelAllocationStrategy implements AllocationStrategy {
	private final int maxNumCores;
	public FullDataParallelAllocationStrategy(int maxNumCores) {
		checkArgument(maxNumCores >= 1);
		this.maxNumCores = maxNumCores;
	}

	@Override
	public int maxNumCores() {
		return maxNumCores;
	}

	@Override
	public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder) {
		//no parameters necessary
	}

	@Override
	public void allocateGroup(ActorGroup group, Range<Integer> iterations, List<Core> cores, Configuration config) {
		int coresSize = Math.min(cores.size(), maxNumCores);
		int perCore = IntMath.divide(iterations.upperEndpoint() - iterations.lowerEndpoint(), coresSize, RoundingMode.CEILING);
		for (int i = 0; i < coresSize && !iterations.isEmpty(); ++i) {
			int min = iterations.lowerEndpoint();
			Range<Integer> allocation = group.isStateful() ? iterations :
					iterations.intersection(Range.closedOpen(min, min + perCore));
			cores.get(i).allocate(group, allocation);
			iterations = Range.closedOpen(allocation.upperEndpoint(), iterations.upperEndpoint());
		}
		assert iterations.isEmpty();
	}
}
