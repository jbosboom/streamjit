package edu.mit.streamjit.impl.compiler;

/**
 * Implementations of this interface can be logically associated with a parent
 * object.  Parented objects have at most one parent, but may have no parent.
 * The parent may be fixed for the life of the object, or may change.
 *
 * This interface deliberately does not provide a method for setting the parent.
 * How the parent is set is up to the implementor of the parent and parented
 * classes.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/12/2013
 */
public interface Parented<P> {
	/**
	 * Returns this object's parent, or null if this object has no parent.
	 * @return this object's parent, or null if this object has no parent
	 */
	public P getParent();
}
