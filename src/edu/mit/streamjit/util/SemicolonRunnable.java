package edu.mit.streamjit.util;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 12/3/2013
 */
public final class SemicolonRunnable implements Runnable {
	private final Iterable<? extends Runnable> runnables;
	public SemicolonRunnable(Iterable<? extends Runnable> runnables) {
		this.runnables = runnables;
	}
	@Override
	public void run() {
		for (Runnable r : runnables)
			r.run();
	}
}
