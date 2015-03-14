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
package edu.mit.streamjit.api;

/**
 * Visitor for StreamElements.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 11/22/2012
 */
public abstract class StreamVisitor {
	private int depth = 0;

	/**
	 * Called when visitation begins, before any StreamElements are visited.
	 */
	public abstract void beginVisit();

	/**
	 * Visits a filter (stateless or stateful).
	 * @param filter a filter
	 */
	public abstract void visitFilter(Filter<?, ?> filter);

	/**
	 * Called when entering a pipeline. Implementations may return true to visit
	 * subelements of the pipeline or false to skip subelements. exitPipeline()
	 * will only be called if true is returned.
	 * @param pipeline a pipeline
	 * @return true to visit subelements, false to skip them
	 */
	public abstract boolean enterPipeline(Pipeline<?, ?> pipeline);

	/**
	 * Called when exiting a pipeline for which true was returned from the
	 * corresponding call to enterPipeline(). If false was returned, this method
	 * is not called.
	 * @param pipeline a pipeline
	 */
	public abstract void exitPipeline(Pipeline<?, ?> pipeline);

	/**
	 * Called when entering a splitjoin. Implementations may return true to
	 * visit subelements of the splitjoin or false to skip subelements.
	 * exitSplitjoin() will only be called if true is returned.
	 * @param splitjoin a splitjoin
	 * @return true to visit subelements, false to skip them
	 */
	public abstract boolean enterSplitjoin(Splitjoin<?, ?> splitjoin);

	/**
	 * Visits a splitter.
	 * @param splitter a splitter
	 */
	public abstract void visitSplitter(Splitter<?, ?> splitter);

	/**
	 * Called when entering a splitjoin branch. Implementations may return true
	 * to visit the branch or false to skip it. exitSplitjoinBranch() will only
	 * be called if true is returned. Other branches of the splitjoin may be
	 * entered regardless of the return value.
	 * @param element a splitjoin branch
	 * @return true to visit the branch, false to skip it
	 */
	public abstract boolean enterSplitjoinBranch(OneToOneElement<?, ?> element);

	/**
	 * Called when exiting a splitjoin branch for which true was returned from
	 * the correspoding call to enterPipeline(). If false was returned, this
	 * method is not called.
	 * @param element a splitjoin branch
	 */
	public abstract void exitSplitjoinBranch(OneToOneElement<?, ?> element);

	/**
	 * Visits a joiner.
	 * @param joiner a joiner
	 */
	public abstract void visitJoiner(Joiner<?, ?> joiner);

	/**
	 * Called when exiting a splitjoin for which true was returned from the
	 * corresponding call to enterSplitjoin(). If false was returned, this
	 * method is not called.
	 * @param splitjoin a splitjoin
	 */
	public abstract void exitSplitjoin(Splitjoin<?, ?> splitjoin);

	/**
	 * Called when visitation ends, after all StreamElements have been visited.
	 */
	public abstract void endVisit();

	/**
	 * Called when visiting a stream element for which this visitor has not
	 * overridden the corresponding visit method. Implementations may return
	 * true if they wish to visit subelements of this element, false if they
	 * wish to skip this element and its subelements, or may throw an exception
	 * (the default).
	 * @param e the unknown element
	 * @return true to visit subelements, false to skip them
	 */
	public boolean visitUnknown(StreamElement<?, ?> e) {
		throw new UnsupportedOperationException(this.getClass().getSimpleName() + " visiting unknown element: " + e);
	}

	private void enter() {
		if (depth == 0)
			beginVisit();
		++depth;
	}

	private void exit() {
		--depth;
		if (depth == 0)
			endVisit();
	}

	/* Any method that might trigger beginVisit or endVisit needs to use
	 * enter()/exit(); the API classes call these package-private methods to
	 * use this handling rather than directly calling into client code.
	 *
	 * If the enterFoo methods return false, we won't call exitFoo, so we call
	 * exit() immediately.  Otherwise we wait until exitFoo().
	 */

	void visitFilter0(Filter<?, ?> filter) {
		enter();
		visitFilter(filter);
		exit();
	}

	boolean enterPipeline0(Pipeline<?, ?> pipeline) {
		enter();
		if (enterPipeline(pipeline))
			return true;
		exit();
		return false;
	}

	void exitPipeline0(Pipeline<?, ?> pipeline) {
		exitPipeline(pipeline);
		exit();
	}

	boolean enterSplitjoin0(Splitjoin<?, ?> splitjoin) {
		enter();
		if (enterSplitjoin(splitjoin))
			return true;
		exit();
		return false;
	}

	void exitSplitjoin0(Splitjoin<?, ?> splitjoin) {
		exitSplitjoin(splitjoin);
		exit();
	}
}
