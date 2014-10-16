package edu.mit.streamjit.util.affinity;

import com.google.common.math.LongMath;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/30/2014
 */
final class NullAffinityStrategy implements AffinityStrategy {
	@Override
	public long getThreadAffinity() {
		return getMaximalAffinityMask();
	}
	@Override
	public void setThreadAffinity(long mask) {
		//do nothing
	}

	@Override
	public long getProcessAffinity() {
		return getMaximalAffinityMask();
	}

	@Override
	public void setProcessAffinity(long mask) {
		//do nothing
	}

	@Override
	public long getMaximalAffinityMask() {
		return LongMath.pow(2, Runtime.getRuntime().availableProcessors())-1;
	}
}
