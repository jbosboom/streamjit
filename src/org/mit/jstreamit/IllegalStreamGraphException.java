package org.mit.jstreamit;

import java.util.Arrays;

/**
 * Thrown when a stream graph is malformed.  This indicates a programming error
 * in the code that built the stream graph or in the implementation of one or
 * more StreamElements in the graph.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/20/2012
 */
public final class IllegalStreamGraphException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	/**
	 * A list of one or more stream elements involved in the malformation, to
	 * aid in debuggging.
	 */
	private final StreamElement<?, ?>[] elements;

	public IllegalStreamGraphException(Throwable cause) {
		this(null, cause);
	}

	public IllegalStreamGraphException(String message, StreamElement<?, ?>... elements) {
		this(message, null, elements);
	}

	public IllegalStreamGraphException(String message, Throwable cause, StreamElement<?, ?>... elements) {
		super(message, cause);
		this.elements = elements.clone();
	}

	/**
	 * Returns an array of elements involved in the malformation, to aid in
	 * debugging.  May be a 0-length array.
	 * @return an array of elements involved in the malformation
	 */
	public StreamElement<?, ?>[] getElements() {
		return elements.clone();
	}

	@Override
	public String getMessage() {
		String msg = super.getMessage();
		if (elements.length == 0)
			return msg;
		if (msg == null)
			return Arrays.toString(elements);
		return msg + ": " + Arrays.toString(elements);
	}
}
