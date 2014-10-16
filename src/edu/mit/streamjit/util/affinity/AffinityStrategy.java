package edu.mit.streamjit.util.affinity;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/30/2014
 */
interface AffinityStrategy {
	public long getThreadAffinity();
	public void setThreadAffinity(long mask);

	public long getProcessAffinity();
	public void setProcessAffinity(long mask);

	/**
	 * Returns an affinity mask containing all processors in the system this
	 * thread or process could possibly execute on.
	 * @return the maximal affinity mask
	 */
	public long getMaximalAffinityMask();
}
