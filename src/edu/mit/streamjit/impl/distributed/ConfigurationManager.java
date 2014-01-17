package edu.mit.streamjit.impl.distributed;

import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.distributed.node.StreamNode;

/**
 * ConfigurationManager deals with {@link Configuration}. Mainly, It does
 * following two tasks.
 * <ol>
 * <li>Generates configuration for with appropriate tuning parameters for
 * tuning.
 * <li>Dispatch the configuration given by the open tuner and make blobs
 * accordingly.
 * </ol>
 * 
 * One can implement this interface to try different search space designs as
 * they want.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Jan 16, 2014
 * 
 */
public interface ConfigurationManager {

	/**
	 * Generates default configuration with all tuning parameters for tuning.
	 * 
	 * @param streamGraph
	 * @param source
	 * @param sink
	 * @param noOfPartitions
	 * @return
	 */
	public Configuration getDefaultConfiguration(
			OneToOneElement<?, ?> streamGraph, Worker<?, ?> source,
			Worker<?, ?> sink, int noOfPartitions);

	/**
	 * When opentuner gives a new configuration, this method may be called to
	 * interpret the configuration and execute the steramjit app with the new
	 * configuration.
	 * 
	 * @param config
	 *            configuration from opentuner.
	 * @return true iff valid configuration is passed.
	 */
	public boolean newConfiguration(Configuration config);

	/**
	 * Generates static information of the app that is needed by steramnodes.
	 * This configuration will be sent to streamnodes when setting up a new app
	 * for execution (Only once).
	 * 
	 * @return static information of the app that is needed by steramnodes.
	 */
	public Configuration getStaticConfiguration();

	/**
	 * For every reconfiguration, this method may be called by the appropriate
	 * class to get new configuration information that can be sent to all
	 * participating {@link StreamNode}s.
	 * 
	 * @return new partition information
	 */
	public Configuration getDynamicConfiguration();
}
