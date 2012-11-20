package org.mit.jstreamit;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/7/2012
 */
public interface StreamElement<I, O> {
	/**
	 * Returns a deep copy of this object.  After this method returns, calls to
	 * other methods on this object have no effect on the returned object, and
	 * vice versa.  Additionally, even for stateless objects, a different object
	 * must be returned (that is, for all x, x != x.copy()).
	 *
	 * Implementations should refine the return type of this method; that is,
	 * myStreamElement.copy() should return a MyStreamElement rather than just a
	 * StreamElement.
	 *
	 * Implementation note: Cloneable is fraught with peril (see Josh Bloch's
	 * Effective Java, Second Edition, Item 11), and the standard replacement of
	 * a copy constructor or static method doesn't work here because we need a
	 * copy with the same dynamic type as this object, thus we need an instance
	 * method.
	 * @return a deep copy of this object
	 */
	public StreamElement<I, O> copy();
}
