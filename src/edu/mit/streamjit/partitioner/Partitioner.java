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
package edu.mit.streamjit.partitioner;

import java.util.List;
import java.util.Set;

import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;

/**
 * Partitioner partitions a stream graph (i.e. {@link OneToOneElement}) in to
 * multiple non overlapping chunks.
 * 
 * Note: In prior to get service from a Partitioner, all workers in the stream
 * graph should be connected by setting all predecessors, successors. Consider
 * using {@link ConnectWorkersVisitor} to set all predecessors, successors,
 * relationships before start partitioning.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Apr 2, 2013
 */
public interface Partitioner<I, O> {

	/**
	 * Partitions a stream graph (i.e. {@link OneToOneElement}) in to equal non
	 * overlapping chunks. If it is not possible to split the stream graph in to
	 * exactly equal chunks, then the last partition can be a residue.
	 * 
	 * @param streamGraph
	 *            : Stream graph that needs to be partitioned.
	 * @param source
	 *            : Source {@link Worker} of the stream graph.
	 * @param sink
	 *            : Sink {@link Worker} of the stream graph.
	 * @param noOfPartitions
	 *            : Number of partitions needed.
	 * @return A list of set of {@link Worker}s is returned.
	 */
	public List<Set<Worker<?, ?>>> partitionEqually(
			OneToOneElement<I, O> streamGraph, Worker<I, ?> source,
			Worker<?, O> sink, int noOfPartitions);
}
