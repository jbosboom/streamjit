package edu.mit.streamjit.impl.compiler;

/**
 * An element to which access control kinds can be applied.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/3/2013
 */
public interface Accessible {
	public Access getAccess();
	public void setAccess(Access access);
}
