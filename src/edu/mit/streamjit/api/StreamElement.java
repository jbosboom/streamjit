package edu.mit.streamjit.api;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/7/2012
 */
public interface StreamElement<I, O> {
	/**
	 * Initiates a visitation by the given visitor over the stream graph rooted
	 * at this element.
	 * @param v a visitor
	 */
	public void visit(StreamVisitor v);
}
