package edu.mit.streamjit.impl.distributed.runtimer;

import edu.mit.streamjit.impl.common.Configuration;

/**
 * Prognosticates the {@link Configuration}s given by the OpenTuner and tell
 * whether a {@link Configuration} is more likely to give a better search
 * objective improvement or not. Depends on the prognosticated information,
 * {@link OnlineTuner} may reconfigure the application or reject the
 * configuration. Currently, the search objective is performance optimization.
 * In future, some other resource optimization objectives may be added (e.g.,
 * Energy minimization).
 * 
 * @author sumanan
 * @since 6 Jan, 2015
 */
public interface ConfigurationPrognosticator {

	/**
	 * Prognosticate a {@link Configuration} and tell whether a
	 * {@link Configuration} is more likely to give a better search objective
	 * improvement or not.
	 * 
	 * @param config
	 * @return {@code true} iff the config is more likely to give a better
	 *         search objective improvement.
	 */
	public boolean prognosticate(Configuration config);

	/**
	 * An auxiliary method that can be used to update a configuration's running
	 * time. Has been added for data analysis purpose.
	 * 
	 * @param time
	 */
	public void time(double time);

	/**
	 * No Prognostication. The method {@link #prognosticate(Configuration)}
	 * always returns {@code true}
	 */
	public static final class NoPrognostication implements
			ConfigurationPrognosticator {

		@Override
		public boolean prognosticate(Configuration config) {
			return true;
		}

		@Override
		public void time(double time) {
		}
	}
}
