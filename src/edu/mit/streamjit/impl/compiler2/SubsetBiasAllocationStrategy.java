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

import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Workers;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * An AllocationStrategy using two parameters per worker: a core count and an
 * ordered list (permutation) of cores to assign to, effectively naming a subset
 * of the cores (but note there are more permutations than subsets).  An
 * additional count selects some cores to transfer part of their work assignment
 * to the remaining cores.
 *
 * The subset strategy was Jason Ansel's idea; the bias parameter was proposed
 * earlier by Saman Amarasinghe.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/5/2014
 */
public class SubsetBiasAllocationStrategy implements AllocationStrategy {
	private final int maxNumCores;
	public SubsetBiasAllocationStrategy(int maxNumCores) {
		this.maxNumCores = maxNumCores;
	}
	@Override
	public int maxNumCores() {
		return maxNumCores;
	}

	@Override
	public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder) {
		ImmutableList.Builder<Integer> universeBuilder = ImmutableList.builder();
		for (int i = 0; i < maxNumCores(); ++i)
			universeBuilder.add(i);
		ImmutableList<Integer> universe = universeBuilder.build();

		for (Worker<?, ?> w : workers) {
			int id = Workers.getIdentifier(w);
			List<Configuration.Parameter> parameters = new ArrayList<>(4);
			parameters.add(new Configuration.IntParameter("Group"+id+"CoreCount", 1, maxNumCores(), maxNumCores()));
			parameters.add(new Configuration.PermutationParameter<>("Group"+id+"CoreOrder", Integer.class, universe));
			parameters.add(new Configuration.IntParameter("Group"+id+"BiasCount", 0, maxNumCores(), 0));
			parameters.add(new Configuration.FloatParameter("Group"+id+"Bias", 0, 1, 0));

			String[] extraData = new String[parameters.size()];
			for (int i = 0; i < parameters.size(); ++i) {
				builder.addParameter(parameters.get(i));
				extraData[i] = parameters.get(i).getName();
			}
			builder.putExtraData("AllocationParamNames"+id, extraData);
		}
	}

	@Override
	public void allocateGroup(ActorGroup group, Range<Integer> iterations, List<Core> cores, Configuration config) {
		int id = group.id();
		int numCores = config.getParameter("Group"+id+"CoreCount", Configuration.IntParameter.class).getValue();
		Configuration.PermutationParameter<Integer> coreOrderParam = config.getParameter("Group"+id+"CoreOrder", Configuration.PermutationParameter.class, Integer.class);
		ImmutableList<? extends Integer> coreOrder = coreOrderParam.getUniverse();
		int rawBiasCount = config.getParameter("Group"+id+"BiasCount", Configuration.IntParameter.class).getValue();
		int biasCount = Math.min(rawBiasCount, numCores-1);
		float bias = config.getParameter("Group"+id+"Bias", Configuration.FloatParameter.class).getValue();

		try {
			List<Core> subset = new ArrayList<>(numCores);
			for (int i = 0; i < coreOrder.size() && subset.size() < numCores; ++i)
				if (coreOrder.get(i) < cores.size())
					subset.add(cores.get(coreOrder.get(i)));
			List<Core> biasSubset = new ArrayList<>(biasCount);
			while (biasSubset.size() < biasCount)
				biasSubset.add(subset.remove(0));

			float deficitFraction = biasCount*(1-bias)/numCores, surplusFraction = 1 - deficitFraction;
			assert deficitFraction >= 0 && surplusFraction >= 0 : String.format("%d %d %f -> %f %f", numCores, biasCount, bias, deficitFraction, surplusFraction);
			iterations = iterations.canonical(DiscreteDomain.integers());
			int totalIterations = iterations.upperEndpoint() - iterations.lowerEndpoint();
			int biasIterations = (int)(totalIterations*deficitFraction);
			//We pass a null config to ensure we don't interfere with the other strategy.
			if (biasCount > 0)
				new FullDataParallelAllocationStrategy(biasCount).allocateGroup(group,
						Range.closedOpen(iterations.lowerEndpoint(), iterations.lowerEndpoint() + biasIterations),
						biasSubset, null);
			if (numCores - biasCount > 0)
				new FullDataParallelAllocationStrategy(numCores - biasCount).allocateGroup(group,
						Range.closedOpen(iterations.lowerEndpoint() + biasIterations, iterations.upperEndpoint()),
						subset, null);
		} catch (Exception ex) {
			StringBuilder sb = new StringBuilder();
			sb.append(iterations).append(" of ").append(group).append("\n");
			sb.append(String.format("numCores %d, raw biasCount %d, biasCount %d, bias %f, order %s", numCores, rawBiasCount, biasCount, bias, coreOrder));
			throw new RuntimeException(sb.toString(), ex);
		}
	}
}
