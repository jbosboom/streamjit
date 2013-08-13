package edu.mit.streamjit.api;

/**
 * Thrown when a StreamCompiler fails to compile a valid, supported stream
 * graph.  For example, the stream graph may have many rate mismatches, causing
 * the steady-state schedule or buffering requirements to be too large.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/12/2013
 */
public final class StreamCompilationFailedException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	public StreamCompilationFailedException() {
	}
	public StreamCompilationFailedException(String message) {
		super(message);
	}
	public StreamCompilationFailedException(String message, Throwable cause) {
		super(message, cause);
	}
	public StreamCompilationFailedException(Throwable cause) {
		super(cause);
	}
}
