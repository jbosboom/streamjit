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
import com.google.common.math.IntMath;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Workers;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * An AllocationStrategy using two parameters per worker: a core count and an
 * ordered list (permutation) of cores to assign to, effectively naming a subset
 * of the cores (but note there are more permutations than subsets).  An
 * additional count selects some cores to transfer part of their work assignment
 * to the remaining cores.
 *
 * To deal with the fusion parameters causing us to select different sets of
 * allocation parameters, we average all parameters in the group here.  This
 * adds some inertia to the tuning (as a change in one parameter is damped by
 * averaging with the others), but results in a change to any parameter in the
 * group having at least some effect.
 *
 * The subset strategy was Jason Ansel's idea; the bias parameter was proposed
 * earlier by Saman Amarasinghe; averaging was suggested by both Saman and
 * Vladimir Kiriansky, after which I finally agreed to try it.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/5/2014
 */
public class SubsetBiasAverageAllocationStrategy implements AllocationStrategy {
	private final int maxNumCores;
	public SubsetBiasAverageAllocationStrategy(int maxNumCores) {
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
			builder.addParameter(new Configuration.IntParameter("Group"+id+"CoreCount", 1, maxNumCores(), maxNumCores()));
			builder.addParameter(new Configuration.PermutationParameter<>("Group"+id+"CoreOrder", Integer.class, universe));
			builder.addParameter(new Configuration.IntParameter("Group"+id+"BiasCount", 0, maxNumCores(), 0));
			builder.addParameter(new Configuration.FloatParameter("Group"+id+"Bias", 0, 1, 0));
		}
	}

	@Override
	public void allocateGroup(ActorGroup group, Range<Integer> iterations, List<Core> cores, Configuration config) {
		int numCores = 0, biasCount = 0;
		List<ImmutableList<? extends Integer>> coreOrders = new ArrayList<>();
		float bias = 0;
		for (Actor a : group.actors()) {
			int id = a.id();
			numCores += config.getParameter("Group"+id+"CoreCount", Configuration.IntParameter.class).getValue();
			Configuration.PermutationParameter<Integer> coreOrderParam = config.getParameter("Group"+id+"CoreOrder", Configuration.PermutationParameter.class, Integer.class);
			coreOrders.add(coreOrderParam.getUniverse());
			int ourBiasCount = config.getParameter("Group"+id+"BiasCount", Configuration.IntParameter.class).getValue();
			biasCount += Math.min(ourBiasCount, numCores-1);
			bias += config.getParameter("Group"+id+"Bias", Configuration.FloatParameter.class).getValue();
		}
		numCores = IntMath.divide(numCores, group.actors().size(), RoundingMode.CEILING);
		biasCount = IntMath.divide(biasCount, group.actors().size(), RoundingMode.FLOOR);
		bias /= group.actors().size();
		//Transpose coreOrders.
		List<Integer> coreOrder = new ArrayList<>();
		for (int i = 0; i < coreOrders.get(0).size(); ++i)
			for (int j = 0 ; j < coreOrders.size(); ++j)
				coreOrder.add(coreOrders.get(j).get(i));
		//Remove duplicates preserving order.
		coreOrder = new ArrayList<>(new LinkedHashSet<>(coreOrder));

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
	}
}
