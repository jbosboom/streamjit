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
 * returns null, the two workers will not be connected with a channel.
 *
 * This class uses lots of raw types to avoid having to recapture the
 * unbounded wildcards all the time.
 *
 * TODO: this should be split into two visitors, one that just connects (in
 * impl.common) and one that adds channels (in impl.interp).  Maybe more
 * generally a "for every pred-succ pair" worker?
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
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
		private List<Worker<?, ?>> branchEnds = new ArrayList<>();
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
			Channel c = channelFactory.makeChannel(w, joiner);
			if (c != null) {
				Workers.addSuccessor(w, joiner, c);
				Workers.addPredecessor(joiner, w, c);
			} else {
				Workers.getSuccessors(w).add(joiner);
				Workers.getPredecessors(joiner).add(w);
			}
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
		if (cur == null) { //First worker encountered.
			source = worker;
		} else {
			Channel c = channelFactory.makeChannel(cur, worker);
			if (c != null) {
				Workers.addSuccessor(cur, worker, c);
				Workers.addPredecessor(worker, cur, c);
			} else {
				Workers.getSuccessors(cur).add(worker);
				Workers.getPredecessors(worker).add(cur);
			}
		}
		cur = worker;
	}

	public final void endVisit() {
		//Ensure all but the first worker aren't sources.
		for (Worker<?, ?> worker : Workers.getAllSuccessors(source))
			for (Rate rate : worker.getPopRates())
				if (rate.max() == 0)
					throw new IllegalStreamGraphException("Source isn't first worker", (StreamElement)cur);
		//Ensure all but the last worker aren't sinks.
		for (Worker<?, ?> worker : Workers.getAllPredecessors(source))
			for (Rate rate : worker.getPopRates())
				if (rate.max() == 0)
					throw new IllegalStreamGraphException("Source isn't first worker", (StreamElement)cur);
	}
}
