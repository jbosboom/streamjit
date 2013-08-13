package edu.mit.streamjit.util.ilpsolve;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/12/2013
 */
public class SolverException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	public SolverException() {
	}
	public SolverException(String message) {
		super(message);
	}
	public SolverException(String message, Throwable cause) {
		super(message, cause);
	}
	public SolverException(Throwable cause) {
		super(cause);
	}
}
