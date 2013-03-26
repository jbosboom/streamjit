package edu.mit.streamjit.util.json;

/**
 * A JsonifierFactory creates Jsonifier instances for a particular type.
 *
 * A JsonifierFactory need not create a new instance for each request, and it
 * may return the same instance to several different requests if that instance
 * can handle all the types.
 *
 * Instances of this class should be thread-safe and reentrant; that is, methods
 * on this class may be called simultaneously by any number of threads,
 * including multiple times from a single thread (recursively).
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/25/2013
 */
public interface JsonifierFactory {
	/**
	 * Gets a Jsonifier instance for the given type, or null if this factory
	 * does not support the given type.  This is typically used when
	 * serializing.
	 * @param <T> the type
	 * @param klass the type
	 * @return a Jsonifier instance for the type, or null
	 */
	public <T> Jsonifier<T> getJsonifier(Class<T> klass);
}
