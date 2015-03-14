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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Workers;

/**
 * {@link HorizontalPartitioner} cuts the stream graph with horizontal lines.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Apr 2, 2013
 */
public class HorizontalPartitioner<I, O> extends AbstractPartitioner<I, O> {

	/**
	 * levelMap is a map that contains set of workers at each level in the
	 * stream graph. Key of levelMap is level. level starts from 0,
	 * (computer scientest's convention).
	 */
	private Map<Integer, Set<Worker<?, ?>>> levelMap;

	public List<Set<Worker<?, ?>>> PatririonEquallyImplementation(
			OneToOneElement<I, O> streamGraph, Worker<I, ?> source,
			Worker<?, O> sink, int noOfPartitions) {

		buildLevelMap(source);
		int partitionSize = (int) Math.ceil((double) graphDepth
				/ noOfPartitions);
		List<Set<Worker<?, ?>>> partitioinList = new ArrayList<>();

		int endLevel;
		for (int i = 0; i < noOfPartitions; i++) {
			endLevel = graphDepth > (i + 1) * partitionSize ? (i + 1)
					* partitionSize : graphDepth;
			partitioinList.add(getWorkers(i * partitionSize, endLevel));
		}
		return partitioinList;
	}

	/**
	 * Treat the streamgraph as a top to bottom hierarchy and returns workers at
	 * the given level range. return all workers those falls
	 * in "startLevel<= level < endLevel" range.
	 * 
	 * @param startLevel
	 * @param endLevel
	 * @return set of workers those falls in "startLevel<= level < endLevel"
	 *         range.
	 */
	private Set<Worker<?, ?>> getWorkers(int startLevel, int endLevel) {
		assert endLevel >= startLevel : String
				.format("endLevel = %d, startLevel = %d, endLevel is lesser than startLevel",
						endLevel, startLevel);
		assert endLevel <= graphDepth : String
				.format("endLevel = %d, graphDepth = %d, endLevel is greater than graphDepth",
						endLevel, graphDepth);

		Set<Worker<?, ?>> workersSubset = new HashSet<>();

		assert levelMap != null;
		assert levelMap.size() == graphDepth : "Missmatch in levelMap size";

		for (int i = startLevel; i < endLevel; i++) {
			workersSubset.addAll(levelMap.get(i));
		}
		return workersSubset;
	}

	/**
	 * Fill the levelMap by the workers in the streamgraph.
	 * 
	 * @param source
	 *            : Source of the stream graph
	 */
	private void buildLevelMap(Worker<?, ?> source) {
		if (levelMap == null) {
			levelMap = new HashMap<Integer, Set<Worker<?, ?>>>();
			for (int i = 0; i < graphDepth; i++) {
				levelMap.put(i, new HashSet<Worker<?, ?>>());
			}
		}
		Worker<?, ?> cur = source;
		int level = 0;
		while (true) {
			if (cur instanceof Filter<?, ?>) {
				levelMap.get(level).add(cur);
				level++;
			} else if (cur instanceof Splitter<?, ?>) {
				buildlevelMapSplitJoin((Splitter<?, ?>) cur, level,
						this.levelMap);
				level += getDepthofSplitJoin((Splitter<?, ?>) cur);
				cur = getJoiner((Splitter<?, ?>) cur);
			} else {
				throw new AssertionError(
						"Either Filter or Splitter needs to come here in the while loop");
			}

			if (Workers.getSuccessors(cur).isEmpty())
				break;
			else
				cur = Workers.getSuccessors(cur).get(0);
		}
		assert level == graphDepth : "Error in algorithm";
	}

	/**
	 * This builds the level map for a splitjoin entity. This recursive function
	 * handles nested splitjoins as well. So just passing top
	 * level splitjoin is far enough to build the level map. The function
	 * buildLevelMap may get help from this function when building
	 * the level map.
	 * 
	 * @param splitter
	 *            : Top level splitter.
	 * @param spliterLevel
	 *            : level where the splitter is located in the stream graph.
	 * @param levelMap
	 */
	private void buildlevelMapSplitJoin(Splitter<?, ?> splitter,
			int spliterLevel, Map<Integer, Set<Worker<?, ?>>> levelMap) {
		assert graphDepth == levelMap.size();

		int curLevel = spliterLevel;
		Joiner<?, ?> joiner = getJoiner(splitter);
		int joinerLevel = spliterLevel + getDepthofSplitJoin(splitter) - 1;

		levelMap.get(curLevel).add(splitter);
		levelMap.get(joinerLevel).add(joiner);

		for (int i = 0; i < Workers.getSuccessors(splitter).size(); i++) {
			Worker<?, ?> cur = Workers.getSuccessors(splitter).get(i);
			curLevel = spliterLevel + 1;
			while (!cur.equals(joiner)) {
				levelMap.get(curLevel).add(cur);
				if (cur instanceof Filter<?, ?>) {
					cur = Workers.getSuccessors(cur).get(0);
					curLevel++;
				} else if (cur instanceof Splitter<?, ?>) {
					buildlevelMapSplitJoin((Splitter<?, ?>) cur, curLevel,
							levelMap);
					curLevel += getDepthofSplitJoin((Splitter<?, ?>) cur);
					cur = Workers
							.getSuccessors(getJoiner((Splitter<?, ?>) cur))
							.get(0);
				} else if (cur instanceof Joiner<?, ?>) {
					System.out.println("Joiner Encounted...Check the algo");
				}
			}
		}
	}
}
