package edu.mit.streamjit.util;

/**
 * Throws checked exceptions as if unchecked.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/13/2014
 */
public final class SneakyThrows {
	private SneakyThrows() {}

	/**
	 * Throws the given Throwable, even if it's a checked exception the caller
	 * could not otherwise throw.
	 *
	 * This method returns RuntimeException to enable "throw sneakyThrow(t);"
	 * syntax to convince Java's dataflow analyzer that an exception will be
	 * thrown.
	 *
	 * Note that catching sneakythrown exceptions can be difficult as Java will
	 * complain about attempts to catch checked exceptions that "cannot" be
	 * thrown from the try-block body.
	 * @param t the Throwable to throw
	 * @return never returns
	 */
	@SuppressWarnings("deprecation")
	public static RuntimeException sneakyThrow(Throwable t) {
		Thread.currentThread().stop(t);
		throw new AssertionError();
	}
}
