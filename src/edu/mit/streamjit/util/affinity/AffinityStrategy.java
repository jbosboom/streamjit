package edu.mit.streamjit.util.affinity;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 1/30/2014
 */
interface AffinityStrategy {
	public long get();
	public void set(long mask);
}
