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
	 * levelMap is a map that contains set of workers of each level in the stream graph. Key of levelMap is level. Level is start with
	 * 1. not with 0.
	 */
	Map<Integer, Set<Worker<?, ?>>> levelMap;

	public List<Set<Worker<?, ?>>> PatririonEqually(OneToOneElement<I, O> streamGraph, int noOfPartitions) {
		preProcessStreamGraph(streamGraph);
		assert graphDepth >= noOfPartitions : "Stream graph's depth is smaller than the number of partitions";
		buildLevelMap(this.source);
		int partitionSize = (int) Math.ceil((double) graphDepth / noOfPartitions);
		List<Set<Worker<?, ?>>> partitioinList = new ArrayList<>();

		int endLevel;
		for (int i = 0; i < noOfPartitions; i++) {
			endLevel = graphDepth > (i + 1) * partitionSize ? (i + 1) * partitionSize : graphDepth;
			partitioinList.add(getWorkers(i * partitionSize + 1, endLevel));
		}
		verifyPartition(partitioinList);
		return partitioinList;
	}

	/**
	 * Treat the streamgraph as a top to bottom hierarchy and returns workers at the given level range.
	 * 
	 * @param startLevel
	 * @param endLevel
	 * @return set of workers from the startLevel upto EndLevel.
	 */
	private Set<Worker<?, ?>> getWorkers(int startLevel, int endLevel) {
		assert endLevel >= startLevel : String.format("endLevel = %d, startLevel = %d, endLevel is lesser than startLevel", endLevel,
				startLevel);
		assert endLevel <= graphDepth : String.format("endLevel = %d, graphDepth = %d, endLevel is greater than graphDepth", endLevel,
				graphDepth);
		
		Set<Worker<?, ?>> workersSubset = new HashSet<>();

		assert levelMap != null;
		assert levelMap.size() == graphDepth : "Missmatch in levelMap size";

		for (int i = startLevel; i <= endLevel; i++) {
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
			for (int i = 1; i <= graphDepth; i++) {
				levelMap.put(i, new HashSet<Worker<?, ?>>());
			}
		}
		Worker<?, ?> cur = source;
		int level = 0;
		while (!cur.equals(sink)) {
			if (cur instanceof Filter<?, ?>) {
				level++;
				levelMap.get(level).add(cur);
				cur = Workers.getSuccessors(cur).get(0);

			} else if (cur instanceof Splitter<?>) {
				buildlevelMapSplitJoin((Splitter<?>) cur, level + 1, this.levelMap);
				level += getDepthofSplitJoin((Splitter<?>) cur);
				Joiner<?> joiner = getJoiner((Splitter<?>) cur);
				cur = Workers.getSuccessors(joiner).isEmpty() ? joiner : Workers.getSuccessors(joiner).get(0);
			} else {
				throw new AssertionError("Either Filter or Splitter needs to come here in the while loop");
			}
		}
		assert level == graphDepth : "Error in algorithm";
		levelMap.get(level).add(sink);
	}

	/**
	 * This builds the level map for a splitjoin entity. This recursive function handles nested splitjoins as well. So just passing top
	 * level splitjoin is far enough to build the level map. The function buildLevelMap can get assist from this function when building
	 * the level map.
	 * 
	 * @param splitter
	 *            : Top level splitter.
	 * @param spliterLevel
	 *            : level where the splitter is located in the stream graph.
	 * @param levelMap
	 */
	private void buildlevelMapSplitJoin(Splitter<?> splitter, int spliterLevel, Map<Integer, Set<Worker<?, ?>>> levelMap) {
		assert graphDepth == levelMap.size();

		int curLevel = spliterLevel;
		Joiner<?> joiner = getJoiner(splitter);
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
				} else if (cur instanceof Splitter<?>) {
					buildlevelMapSplitJoin((Splitter<?>) cur, curLevel, levelMap);
					curLevel += getDepthofSplitJoin((Splitter<?>) cur);
					cur = Workers.getSuccessors(getJoiner((Splitter<?>) cur)).get(0);
				} else if (cur instanceof Joiner<?>) {
					System.out.println("Joiner Encounted...Check the algo");
				}
			}
		}
	}
}
