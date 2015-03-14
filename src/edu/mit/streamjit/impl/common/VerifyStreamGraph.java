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
package edu.mit.streamjit.impl.common;

import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.IllegalStreamGraphException;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.StreamVisitor;
import edu.mit.streamjit.api.WeightedRoundrobinJoiner;
import edu.mit.streamjit.api.WeightedRoundrobinSplitter;
import edu.mit.streamjit.api.Worker;

/**
 * {@link VerifyStreamGraph} currently verifies a stream graph for following
 * correctness. 1) A filter instance should be added only once in the graph. 2)
 * {@link WeightedRoundrobinSplitter} has matching numbers of branches and
 * weights. 3) {@link WeightedRoundrobinJoiner} has matching numbers of weights
 * array and the input branches.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 9, 2013
 */
public class VerifyStreamGraph extends StreamVisitor {

	/**
	 * Stores all visited {@link Worker}s for duplication check.
	 */
	List<Worker<?, ?>> visitedWorkers;

	/**
	 * This stack contains entered, but not exited, {@link Splitjoin}'s
	 * {@link Splitter} and the corresponding count of the entered SplitJoin
	 * branches. As there is no any purposely designed pair or tuple
	 * representations in Java, we use Map.Entry<?,?> to represent a pair.
	 */
	Deque<Map.Entry<Splitter<?, ?>, Integer>> unfinishedSpliterStack;

	public VerifyStreamGraph() {
		visitedWorkers = new LinkedList<>();
		unfinishedSpliterStack = new ArrayDeque<>();
	}

	@Override
	public void beginVisit() {
		visitedWorkers.clear();
		unfinishedSpliterStack.clear();
	}

	@Override
	public void visitFilter(Filter<?, ?> filter) {
		if (visitedWorkers.contains(filter))
			throw new IllegalStreamGraphException(String.format(
					"The filter instanse \"%s\" is added multiple time",
					filter.toString()));
		else
			visitedWorkers.add(filter);
	}

	@Override
	public boolean enterPipeline(Pipeline<?, ?> pipeline) {
		return true;
	}

	@Override
	public void exitPipeline(Pipeline<?, ?> pipeline) {

	}

	@Override
	public boolean enterSplitjoin(Splitjoin<?, ?> splitjoin) {
		return true;
	}

	@Override
	public void visitSplitter(Splitter<?, ?> splitter) {
		if (visitedWorkers.contains(splitter))
			throw new IllegalStreamGraphException(String.format(
					"The splitter instance \"%s\" is added multiple time",
					splitter.toString()));
		else
			visitedWorkers.add(splitter);

		Map.Entry<Splitter<?, ?>, Integer> pair = new AbstractMap.SimpleEntry<Splitter<?, ?>, Integer>(
				splitter, 0);
		unfinishedSpliterStack.push(pair);
	}

	@Override
	public boolean enterSplitjoinBranch(OneToOneElement<?, ?> element) {
		int val = unfinishedSpliterStack.peek().getValue();
		val++;
		unfinishedSpliterStack.peek().setValue(val);
		return true;
	}

	@Override
	public void exitSplitjoinBranch(OneToOneElement<?, ?> element) {
		// TODO Auto-generated method stub

	}

	@Override
	// FIXME: splitter.supportedOutputs() returns Splitter.UNLIMITED. Couldn't
	// get actual numbers of the branches in the splitter.
	public void visitJoiner(Joiner<?, ?> joiner) {
		Map.Entry<Splitter<?, ?>, Integer> pair = unfinishedSpliterStack.pop();
		Splitter<?, ?> splitter = pair.getKey();
		int branchCount = pair.getValue();

		/*
		 * if (splitter.supportedOutputs() != branchCount) throw new
		 * IllegalStreamGraphException(String.format(
		 * "%s splitter's supported output is not equal to its number of branches"
		 * , splitter.toString()));
		 * 
		 * if (joiner.supportedInputs() != branchCount) throw new
		 * IllegalStreamGraphException(String.format(
		 * "%s joiner's supported input is not equal to its number of branches",
		 * joiner.toString()));
		 */
	}

	@Override
	public void exitSplitjoin(Splitjoin<?, ?> splitjoin) {
		// TODO Auto-generated method stub

	}

	@Override
	public void endVisit() {
		if (!unfinishedSpliterStack.isEmpty())
			throw new IllegalStreamGraphException(
					String.format("Un structured stream graph."));
	}

}
