package edu.mit.streamjit.util.affinity;

import com.google.common.math.LongMath;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 1/30/2014
 */
final class NullAffinityStrategy implements AffinityStrategy {
	@Override
	public long get() {
		return LongMath.pow(2, Runtime.getRuntime().availableProcessors())-1;
	}
	@Override
	public void set(long mask) {
		//do nothing
	}
}
