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
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
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
	private void visitWorker(Worker<?, ?> worker) {
		Workers.getPredecessors(worker).clear();
		Workers.getSuccessors(worker).clear();
		Workers.getInputChannels(worker).clear();
		Workers.getOutputChannels(worker).clear();
		Workers.setIdentifier(worker, -1);
	}
}
