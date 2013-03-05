package edu.mit.streamjit;

import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.Filter;

/**
 * Visitor for StreamElements.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/22/2012
 */
public abstract class StreamVisitor {
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
}
