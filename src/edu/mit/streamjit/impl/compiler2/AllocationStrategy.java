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
import java.util.List;
import java.util.Set;

/**
 * A strategy for allocating groups to cores.  Such a strategy involves making
 * parameters and interpreting them.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 12/31/2013
 */
public interface AllocationStrategy {
	/**
	 * The maximum number of cores this strategy can assign to.  For
	 * most strategies, this will be specified on construction, but simple
	 * strategies might set a fixed limit (e.g., a fully-serial strategy, 1).
	 * @return the max number of cores this strategy can assign to
	 */
	public int maxNumCores();

	/**
	 * Adds parameters used by this strategy to the given builder.  The default
	 * values should be a useful default (e.g., a full data-parallelization
	 * across all cores), though that may not be possible for some strategies
	 * (e.g., if multiplier-dependent).
	 * @param workers the workers the configuration is being built for
	 * @param builder the builder
	 */
	public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder);

	/**
	 * Allocates a group to cores based on the configuration.  There may be a
	 * different number of cores than this strategy's maximum, with the strategy
	 * deciding how to accommodate.
	 *
	 * TODO: This doesn't allow the strategy to decide the order the groups are
	 * allocated.  That could be orthogonal to the allocation strategy, but
	 * perhaps a strategy wants to interleave groups (e.g., to ensure produced
	 * outputs are consumed as inputs before they're evicted from the cache)?
	 * @param group the group to allocate
	 * @param iterations the range of iterations to allocate
	 * @param cores the cores to allocate to
	 * @param config the configuration (will contain parameters created in
	 * makeParameters)
	 */
	public void allocateGroup(ActorGroup group, Range<Integer> iterations, List<Core> cores, Configuration config);
}
