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

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.math.DoubleMath;
import com.google.common.primitives.Ints;
import edu.mit.streamjit.api.StatefulFilter;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Workers;
import java.math.RoundingMode;
import java.util.List;
import java.util.Set;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/21/2014
 */
public class CompositionAllocationStrategy implements AllocationStrategy {
	private final int maxNumCores;
	public CompositionAllocationStrategy(int maxNumCores) {
		this.maxNumCores = maxNumCores;
	}

	@Override
	public int maxNumCores() {
		return maxNumCores;
	}

	@Override
	public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder) {
		ImmutableList.Builder<Integer> integersBuilder = ImmutableList.builder();
		for (int i = 0; i < maxNumCores(); ++i)
			integersBuilder.add(i);
		ImmutableList<Integer> integers = integersBuilder.build();

		for (Worker<?, ?> w : workers) {
			int id = Workers.getIdentifier(w);
			//If stateful, which core?
			if (w instanceof StatefulFilter)
				builder.addParameter(new Configuration.SwitchParameter<>("Group"+id+"Core", Integer.class, 0, integers));
			//Otherwise, how to divide?
			else
				builder.addParameter(new Configuration.CompositionParameter("Group"+id+"Cores", maxNumCores()));
		}
	}

	@Override
	public void allocateGroup(ActorGroup group, Range<Integer> iterations, List<Core> cores, Configuration config) {
		if (group.isStateful()) {
			int minStatefulId = Integer.MAX_VALUE;
			for (Actor a : group.actors())
				if (a instanceof WorkerActor && ((WorkerActor)a).archetype().isStateful())
					minStatefulId = Math.min(minStatefulId, a.id());
			Configuration.SwitchParameter<Integer> param = config.getParameter("Group"+minStatefulId+"Core", Configuration.SwitchParameter.class, Integer.class);
			cores.get(param.getValue() % cores.size()).allocate(group, iterations);
			return;
		}

		Configuration.CompositionParameter param = config.getParameter("Group"+group.id()+"Cores", Configuration.CompositionParameter.class);
		assert iterations.lowerBoundType() == BoundType.CLOSED && iterations.upperBoundType() == BoundType.OPEN;
		int totalAvailable = iterations.upperEndpoint() - iterations.lowerEndpoint();
		int[] allocations = new int[cores.size()];
		int totalAllocated = 0;
		for (int i = 0; i < param.getLength() && i < allocations.length; ++i) {
			int allocation = DoubleMath.roundToInt(param.getValue(i) * totalAvailable, RoundingMode.HALF_EVEN);
			allocations[i] = allocation;
			totalAllocated += allocation;
		}
		//If we allocated more than we have, remove from the cores with the least.
		//Need a loop here because we might not have enough on the least core.
		while (totalAllocated > totalAvailable) {
			int least = Ints.indexOf(allocations, Ints.max(allocations));
			for (int i = 0; i < allocations.length; ++i)
				if (allocations[i] > 0 && allocations[i] < allocations[least])
					least = i;
			int toRemove = Math.min(allocations[least], totalAllocated - totalAvailable);
			allocations[least] -= toRemove;
			totalAllocated -= toRemove;
		}
		//If we didn't allocate enough, allocate on the cores with the most.
		if (totalAllocated < totalAvailable) {
			int most = Ints.indexOf(allocations, Ints.min(allocations));
			for (int i = 0; i < allocations.length; ++i)
				if (allocations[i] > allocations[most])
					most = i;
			allocations[most] += totalAvailable - totalAllocated;
			totalAllocated += totalAvailable - totalAllocated;
		}
		assert totalAllocated == totalAvailable : totalAllocated +" "+totalAvailable;

		int lower = iterations.lowerEndpoint();
		for (int i = 0; i < allocations.length; ++i)
			if (allocations[i] > 0) {
				cores.get(i).allocate(group, Range.closedOpen(lower, lower+allocations[i]));
				lower += allocations[i];
			}
	}
}
