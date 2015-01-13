package edu.mit.streamjit.impl.distributed;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.StreamVisitor;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.test.apps.filterbank6.FilterBank6;

/**
 * Generate all stream paths in a stream graph.
 * 
 * @author sumanan
 * @since 13 Jan, 2015
 */
public class StreamPathBuilder {

	/**
	 * streamGraph must be connected before requesting for paths. Use
	 * {@link ConnectWorkersVisitor} to connect the streamGraph.
	 * 
	 * @param streamGraph
	 * @return Set of all paths in the streamGraph.
	 */
	public static Set<List<Integer>> paths(OneToOneElement<?, ?> streamGraph) {
		PathVisitor v = new PathVisitor();
		streamGraph.visit(v);
		return v.currentUnfinishedPathSet;
	}

	private static class PathVisitor extends StreamVisitor {

		/**
		 * Paths those are currently being built. These paths will get extended
		 * as StreamPathBuilder visits through the stream graph.
		 */
		private Set<List<Integer>> currentUnfinishedPathSet;

		/**
		 * Keeps track of paths to all {@link Splitter} encountered in a stack.
		 * Once corresponding {@link Joiner} is visited, path set will be popped
		 * from this stack.
		 */
		private Deque<Set<List<Integer>>> pathToSplitterStack;

		/**
		 * Unfinished path sets which are waiting for a correct joiner. Path set
		 * will be popped from this stack once correct joiner is reached.
		 */
		private Deque<Set<List<Integer>>> waitingForJoinerStack;

		private PathVisitor() {
			currentUnfinishedPathSet = new HashSet<>();
			pathToSplitterStack = new ArrayDeque<>();
			waitingForJoinerStack = new ArrayDeque<>();
		}

		@Override
		public void beginVisit() {
			currentUnfinishedPathSet.clear();
			pathToSplitterStack.clear();
			waitingForJoinerStack.clear();
			currentUnfinishedPathSet.add(new LinkedList<Integer>());
		}

		@Override
		public void visitFilter(Filter<?, ?> filter) {
			addToCurrentPath(filter);
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
			addToCurrentPath(splitter);
			pathToSplitterStack.push(currentUnfinishedPathSet);
			waitingForJoinerStack.push(new HashSet<List<Integer>>());
		}

		@Override
		public boolean enterSplitjoinBranch(OneToOneElement<?, ?> element) {
			currentUnfinishedPathSet = new HashSet<List<Integer>>();
			for (List<Integer> splitterPath : pathToSplitterStack.peek()) {
				currentUnfinishedPathSet.add(new LinkedList<Integer>(
						splitterPath));
			}
			return true;
		}

		@Override
		public void exitSplitjoinBranch(OneToOneElement<?, ?> element) {
			waitingForJoinerStack.peek().addAll(currentUnfinishedPathSet);
		}

		@Override
		public void visitJoiner(Joiner<?, ?> joiner) {
			currentUnfinishedPathSet = waitingForJoinerStack.pop();
			addToCurrentPath(joiner);
			pathToSplitterStack.pop();
		}

		@Override
		public void exitSplitjoin(Splitjoin<?, ?> splitjoin) {
		}

		@Override
		public void endVisit() {
			if (!waitingForJoinerStack.isEmpty())
				throw new IllegalStateException("waitingForJoiner not empty");
			if (!pathToSplitterStack.isEmpty())
				throw new IllegalStateException("pathToSplitter not empty");
			for (List<Integer> path : currentUnfinishedPathSet) {
				for (Integer i : path) {
					System.out.print(i.toString() + "->");
				}
				System.out.println();
			}
		}

		/**
		 * Extends all current unfinished path set with the {@link Worker} w.
		 * 
		 * @param w
		 */
		private void addToCurrentPath(Worker<?, ?> w) {
			int id = Workers.getIdentifier(w);
			for (List<Integer> path : currentUnfinishedPathSet)
				path.add(id);
		}
	}

	public static void main(String[] args) {
		OneToOneElement<?, ?> stream = new FilterBank6.FilterBankPipeline();
		new StreamJitApp<>(stream); // Connects the stream into stream graph.
		StreamPathBuilder.paths(stream);
	}
}