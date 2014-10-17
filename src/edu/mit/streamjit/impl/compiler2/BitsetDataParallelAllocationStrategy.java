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

import com.google.common.collect.Range;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Workers;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * An allocation strategy that maintains a bitset for each group,
 * data-parallelizing evenly over cores whose bits are set.
 *
 * This strategy favors space multiplexing.  Equal data-parallelization can only
 * occur if all N bits are set, and there are more ways to assign two groups to
 * N/2 cores each than both to all cores.  However, within each group's
 * assignment, cores receive equal allocations.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 12/31/2013
 */
public class BitsetDataParallelAllocationStrategy implements AllocationStrategy {
	private final int maxNumCores;
	public BitsetDataParallelAllocationStrategy(int maxNumCores) {
		this.maxNumCores = maxNumCores;
	}

	@Override
	public int maxNumCores() {
		return maxNumCores;
	}

	@Override
	public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder) {
		for (Worker<?, ?> w : workers)
			for (int i = 0; i < maxNumCores(); ++i) {
				String name = String.format("node%dcore%dallocate", Workers.getIdentifier(w), i);
				builder.addParameter(Configuration.SwitchParameter.create(name, true));
			}
	}

	@Override
	public void allocateGroup(ActorGroup group, Range<Integer> iterations, List<Core> cores, Configuration config) {
		int count = Math.min(cores.size(), maxNumCores());
		List<Core> enabled = new ArrayList<>(count);
		for (int i = 0; i < count; ++i) {
			String name = String.format("node%dcore%dallocate", group.id(), i);
			Configuration.SwitchParameter<Boolean> param = config.getParameter(name, Configuration.SwitchParameter.class, Boolean.class);
			if (param.getValue())
				enabled.add(cores.get(i));
		}
		if (enabled.isEmpty())
			enabled.addAll(cores.subList(0, count));
		new FullDataParallelAllocationStrategy(enabled.size()).allocateGroup(group, iterations, enabled, config);
	}
}
