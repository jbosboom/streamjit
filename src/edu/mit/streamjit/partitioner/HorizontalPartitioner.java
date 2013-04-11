package edu.mit.streamjit.partitioner;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Workers;

/**
 * {@link HorizontalPartitioner} cuts the stream graph with horizontal lines.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Apr 2, 2013
 */
public class HorizontalPartitioner<I, O> extends AbstractPartitioner<I, O> {

	private Set<Worker<?, ?>> getWorkers(int startLevel, int endLevel) {
		assert endLevel >= startLevel : String.format("endLevel = %d, startLevel = %d, endLevel is lesser than startLevel", endLevel,
				startLevel);
		assert endLevel <= graphDepth : String.format("endLevel = %d, graphDepth = %d, endLevel is greater than graphDepth", endLevel,
				graphDepth);

		// ImmutableSet.Builder<Worker<?, ?>> workersSubset = ImmutableSet.builder();
		Set<Worker<?, ?>> workersSubset = new HashSet<>();

		int depth = 1; // source is at depth 1
		Worker<?, ?> cur = source;

		// Skip the workers up to startLevel
		while (!Workers.getSuccessors(cur).isEmpty() && depth < startLevel - 1) {
			cur = Workers.getSuccessors(cur).get(0);
			depth++;
		}

		if (startLevel == 1)
			workersSubset.add(source);

		while (depth < endLevel) {
			workersSubset.addAll(Workers.getSuccessors(cur));
			cur = Workers.getSuccessors(cur).get(0);
			depth++;
		}
		// return workersSubset.build();
		return workersSubset;
	}

	@Override
	public List<Set<Worker<?, ?>>> PatririonEqually(OneToOneElement<I, O> streamGraph, int noOfPartitions) {
		preProcessStreamGraph(streamGraph);
		assert graphDepth >= noOfPartitions : "Stream graph's depth is smaller than the number of partitions";
		int partitionSize = (int) Math.ceil(graphDepth / noOfPartitions);
		List<Set<Worker<?, ?>>> partitioinList = new ArrayList<>();

		int endLevel;
		for (int i = 0; i < noOfPartitions; i++) {
			endLevel = graphDepth > (i + 1) * partitionSize ? (i + 1) * partitionSize : graphDepth;
			partitioinList.add(getWorkers(i * partitionSize + 1, endLevel));
		}
		verifyPartition(partitioinList);
		return partitioinList;
	}
}
