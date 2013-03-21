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
import edu.mit.streamjit.impl.interp.EmptyChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Visits the stream graph, connecting the primitive workers with channels
 * and keeping references to the first and last filters.  Subclasses override
 * the makeChannel() method to provide Channels to connect filters together.
 * This visitor does not add input or output channels to the first or last
 * filters respectively.
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
public final class ConnectPrimitiveWorkersVisitor extends StreamVisitor {
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
	private Deque<SplitjoinContext> stack = new ArrayDeque<>();

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
	 * Creates a new ConnectPrimitiveWorkersVisitor that connects workers with
	 * EmptyChannels.  This is useful to discover predecessor/successor
	 * relationships when the graph won't be executed.
	 */
	public ConnectPrimitiveWorkersVisitor() {
		this(new ChannelFactory() {
			@Override
			public <E> Channel<E> makeChannel(Worker<?, E> upstream, Worker<E, ?> downstream) {
				return new EmptyChannel<>();
			}
		});
	}

	/**
	 * Creates a new ConnectPrimitiveWorkersVisitor that connects workers with
	 * channels from the given ChannelFactory.
	 * @param channelFactory the channel factory to use
	 */
	public ConnectPrimitiveWorkersVisitor(ChannelFactory channelFactory) {
		this.channelFactory = checkNotNull(channelFactory);
	}

	/**
	 * After the visitation is complete, returns the first worker in the stream
	 * graph.
	 * @returnthe first worker in the stream graph
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
		//Note that a joiner cannot be the first worker encountered because
		//joiners only occur in splitjoins and the splitter will be visited
		//first.
		for (Worker<?, ?> w : stack.peekFirst().branchEnds) {
			Channel c = channelFactory.makeChannel(w, joiner);
			Workers.addSuccessor(w, joiner, c);
			Workers.addPredecessor(joiner, w, c);
		}

		stack.pop();
		cur = joiner;

		//We can't visit a Joiner outside of a Splitjoin, so no depth check for
		//finishedVisitation() is required.
	}

	@Override
	public final void exitSplitjoin(Splitjoin<?, ?> splitjoin) {
		//We pop the SplitjoinContext in visitJoiner().
	}

	private void visitWorker(Worker worker) {
		if (cur == null) { //First worker encountered.
			source = worker;
		} else {
			Channel c = channelFactory.makeChannel(cur, worker);
			Workers.addSuccessor(cur, worker, c);
			Workers.addPredecessor(worker, cur, c);
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
