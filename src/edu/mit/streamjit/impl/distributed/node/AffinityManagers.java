package edu.mit.streamjit.impl.distributed.node;

import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.impl.blob.Blob;

/**
 * Various implementations of the interface {@link AffinityManager}.
 * 
 * @author sumanan
 * @since 4 Feb, 2015
 */
public class AffinityManagers {

	/**
	 * This is an empty {@link AffinityManager}. {@link #getAffinity(Blob, int)}
	 * always returns null.
	 * 
	 * @author sumanan
	 * @since 4 Feb, 2015
	 */
	public static class EmptyAffinityManager implements AffinityManager {

		@Override
		public ImmutableSet<Integer> getAffinity(Blob blob, int coreCode) {
			return null;
		}
	}
}
