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

import static com.google.common.base.Preconditions.*;
import edu.mit.streamjit.impl.interp.Channel;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.IllegalStreamGraphException;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.api.Rate;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.StreamElement;
import edu.mit.streamjit.api.StreamVisitor;
import edu.mit.streamjit.impl.interp.ChannelFactory;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Visits the stream graph, setting up predecessor-successor relations, setting
 * the worker identifier field, and optionally connecting workers with channels.
 * The provided ChannelFactory is used to create channels; if the factory
 * returns null, the two workers will not be connected with a channel.  This
 * visitor will attempt to create channels for overall stream graph input and
 * output if they don't already exist.
 *
 * Provided the stream graph is not structurally modified (elements not added,
 * removed or reordered), it is safe to use instances of ConnectWorkersVisitor
 * to visit the graph multiple times, for example, to create channels when
 * previous visitations did not (by returning null from
 * ChannelFactory.makeChannel()).  It can be useful to visit once to set up
 * predecessor/successor relationships, build more data structures, then use
 * those data structures to decide what type of channel to create.
 *
 * This class uses lots of raw types to avoid having to recapture the
 * unbounded wildcards all the time.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/23/2013 (originally internal to DebugStreamCompiler)
 */
public final class ConnectWorkersVisitor extends StreamVisitor {
	/**
	 * The ChannelFactory used to create channels.
	 */
	private final ChannelFactory channelFactory;
	/**
	 * The first worker in the stream graph.
	 */
	private Worker<?, ?> source;
	/**
	 * During the visitation, the last worker encountered.  After the visitation
	 * is complete, the last worker in the stream graph.
	 */
	private Worker<?, ?> cur;
	/**
	 * The splitjoin context stack.  See comments on SplitjoinContext.
	 */
	private Deque<SplitjoinContext> stack = new ArrayDeque<>();
	/**
	 * The worker identifier counter, used to give each worker an identifier
	 * unique over the stream graph.
	 */
	private int identifier = 0;

	/**
	 * Used for remembering information about splitjoins while traversing:
	 * so we can reset cur to splitter when we enter a branch, and so we
	 * record cur at the end of a branch to connect it to the joiner after
	 * all branches are processed.
	 */
	private static class SplitjoinContext {
		private Splitter<?, ?> splitter;
		private final List<Worker<?, ?>> branchEnds = new ArrayList<>();
	}

	/**
	 * Creates a new ConnectWorkersVisitor that doesn't connect workers with
	 * channels.  Predecessor-successor relationships and worker identifiers
	 * are still set up.
	 */
	public ConnectWorkersVisitor() {
		this(new ChannelFactory() {
			@Override
			public <E> Channel<E> makeChannel(Worker<?, E> upstream, Worker<E, ?> downstream) {
				return null;
			}
		});
	}

	/**
	 * Creates a new ConnectWorkersVisitor that connects workers with
	 * channels from the given ChannelFactory.
	 * @param channelFactory the channel factory to use
	 */
	public ConnectWorkersVisitor(ChannelFactory channelFactory) {
		this.channelFactory = checkNotNull(channelFactory);
	}

	/**
	 * After the visitation is complete, returns the first worker in the stream
	 * graph.
	 * @return the first worker in the stream graph
	 */
	public final Worker<?, ?> getSource() {
		return source;
	}

	/**
	 * After the visitation is complete, returns the last worker in the stream
	 * graph.
	 * @return last worker in the stream graph
	 */
	public final Worker<?, ?> getSink() {
		return cur;
	}

	@Override
	public final void beginVisit() {
	}

	@Override
	public final void visitFilter(Filter filter) {
		visitWorker(filter);
	}

	@Override
	public final boolean enterPipeline(Pipeline<?, ?> pipeline) {
		return true;
	}

	@Override
	public final void exitPipeline(Pipeline<?, ?> pipeline) {
	}

	@Override
	public final boolean enterSplitjoin(Splitjoin<?, ?> splitjoin) {
		//We push the new SplitjoinContext in visitSplitter().
		return true;
	}

	@Override
	public final void visitSplitter(Splitter splitter) {
		visitWorker(splitter);
		SplitjoinContext ctx = new SplitjoinContext();
		ctx.splitter = splitter;
		stack.push(ctx);
	}

	@Override
	public final boolean enterSplitjoinBranch(OneToOneElement<?, ?> element) {
		//Reset cur to the remembered splitter.
		cur = stack.peek().splitter;
		//Visit subelements.
		return true;
	}

	@Override
	public final void exitSplitjoinBranch(OneToOneElement<?, ?> element) {
		//Remember cur as a branch end.
		stack.peek().branchEnds.add(cur);
	}

	@Override
	public final void visitJoiner(Joiner joiner) {
		Workers.setIdentifier(joiner, identifier++);
		//Note that a joiner cannot be the first worker encountered because
		//joiners only occur in splitjoins and the splitter will be visited
		//first.
		for (Worker<?, ?> w : stack.peekFirst().branchEnds) {
			connect(w, joiner);
		}

		stack.pop();
		cur = joiner;
	}

	@Override
	public final void exitSplitjoin(Splitjoin<?, ?> splitjoin) {
		//We pop the SplitjoinContext in visitJoiner().
	}

	private void visitWorker(Worker worker) {
		Workers.setIdentifier(worker, identifier++);
		if (cur != null)
			connect(cur, worker);
		else {
			//First worker encountered.
			source = worker;
			if (Workers.getInputChannels(worker).isEmpty()) {
				//Create the channel for overall stream graph input.
				Channel c = channelFactory.makeChannel(null, worker);
				if (c != null)
					Workers.getInputChannels(worker).add(c);
			}
		}
		cur = worker;
	}

	private void connect(Worker upstream, Worker downstream) {
		assert upstream != null && downstream != null;
		if (upstream != null && downstream != null && !Workers.getSuccessors(upstream).contains(downstream)) {
			assert !Workers.getPredecessors(downstream).contains(upstream) : "Already connected in one direction only?";
			//The channel may be null.  That's okay.
			Channel c = channelFactory.makeChannel(upstream, downstream);
			Workers.addSuccessor(upstream, downstream, c);
			Workers.addPredecessor(downstream, upstream, c);
			return;
		}

		//We've already connected them, but maybe we should make a channel.
		int upIdx = Workers.getSuccessors(upstream).indexOf(downstream);
		int downIdx = Workers.getPredecessors(downstream).indexOf(upstream);
		if (Workers.getOutputChannels(upstream).get(upIdx) == null) {
			assert Workers.getInputChannels(downstream).get(downIdx) == null : "Channel in one direction only?";
			//The channel may be null.  That's okay -- replace null with null.
			Channel c = channelFactory.makeChannel(upstream, downstream);
			Workers.getOutputChannels(upstream).set(upIdx, c);
			Workers.getInputChannels(downstream).set(downIdx, c);
		}
	}

	public final void endVisit() {
		if (Workers.getOutputChannels(cur).isEmpty()) {
			//Create the channel for overall stream graph output.
			Channel c = channelFactory.makeChannel(cur, null);
			if (c != null)
				Workers.getOutputChannels(cur).add(c);
		}
	}
}
