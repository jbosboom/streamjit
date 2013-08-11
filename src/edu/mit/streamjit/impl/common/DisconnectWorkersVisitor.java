package edu.mit.streamjit.impl.common;

import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.StreamVisitor;
import edu.mit.streamjit.api.Worker;

/**
 * Disconnects workers, including removing channels.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/9/2013
 */
public final class DisconnectWorkersVisitor extends StreamVisitor {
	public DisconnectWorkersVisitor() {
	}
	@Override
	public void beginVisit() {
	}
	@Override
	public void visitFilter(Filter<?, ?> filter) {
		visitWorker(filter);
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
		visitWorker(splitter);
	}
	@Override
	public boolean enterSplitjoinBranch(OneToOneElement<?, ?> element) {
		return true;
	}
	@Override
	public void exitSplitjoinBranch(OneToOneElement<?, ?> element) {
	}
	@Override
	public void visitJoiner(Joiner<?, ?> joiner) {
		visitWorker(joiner);
	}
	@Override
	public void exitSplitjoin(Splitjoin<?, ?> splitjoin) {
	}
	@Override
	public void endVisit() {
	}
	private void visitWorker(Worker worker) {
		Workers.getPredecessors(worker).clear();
		Workers.getSuccessors(worker).clear();
		Workers.getInputChannels(worker).clear();
		Workers.getOutputChannels(worker).clear();
		Workers.setIdentifier(worker, -1);
	}
}
