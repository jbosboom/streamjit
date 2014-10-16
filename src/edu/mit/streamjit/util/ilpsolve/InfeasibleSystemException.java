package edu.mit.streamjit.util.ilpsolve;

/**
 * Thrown when the ILP solver fails to find a solution because the system is
 * infeasible (no solutions exist within the variable bounds).
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 8/12/2013
 */
public final class InfeasibleSystemException extends SolverException {
	private static final long serialVersionUID = 1L;
	public InfeasibleSystemException() {
		super();
	}
	public InfeasibleSystemException(String message) {
		super(message);
	}
}
