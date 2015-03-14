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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import edu.mit.streamjit.api.StatefulFilter;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Workers;
import java.util.List;
import java.util.Set;

/**
 * An AllocationStrategy that uses a count parameter to decide how many cores to
 * spread each group over.
 *
 * This doesn't make any attempt to efficiently bin-pack in the case where not
 * all cores are used (doing so would require either extra parameters for the
 * packing order or a work estimation heuristic).
 *
 * TODO: we could have the parameter range from 1 to maxNumCores()*2-1 to give
 * us a 50% chance of picking the maximum when choosing at random (other weights
 * are obviously possible too).
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/12/2014
 */
public class CountDataParallelAllocationStrategy implements AllocationStrategy {
	private final int maxNumCores;
	public CountDataParallelAllocationStrategy(int maxNumCores) {
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
			//Otherwise, how many cores?
			else
				builder.addParameter(new Configuration.IntParameter("Group"+id+"Cores", 1, maxNumCores(), maxNumCores()));
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
		} else {
			String name = String.format("Group%dCores", group.id());
			Configuration.IntParameter param = config.getParameter(name, Configuration.IntParameter.class);
			int count = Math.min(Math.min(cores.size(), maxNumCores()), param.getValue());
			assert count > 0 : count;
			new FullDataParallelAllocationStrategy(count).allocateGroup(group, iterations, cores.subList(0, count), config);
		}
	}
}
