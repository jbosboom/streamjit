package edu.mit.streamjit.impl.distributed;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.PartitionParameter;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.impl.interp.Interpreter;

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
	 * @param noOfMachines
	 * @return
	 */
	public Configuration getDefaultConfiguration(
			OneToOneElement<?, ?> streamGraph, Worker<?, ?> source,
			Worker<?, ?> sink, int noOfMachines);

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

	/**
	 * Implements the functions those can be called by runtimer to send
	 * configuration information to streamnodes.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since Jan 17, 2014
	 */
	public static abstract class AbstractConfigurationManager
			implements
				ConfigurationManager {

		protected final StreamJitApp app;
		AbstractConfigurationManager(StreamJitApp app) {
			this.app = app;
		}

		@Override
		public Configuration getStaticConfiguration() {
			Configuration.Builder builder = Configuration.builder();
			builder.putExtraData(GlobalConstants.JARFILE_PATH, app.jarFilePath)
					.putExtraData(GlobalConstants.TOPLEVEL_WORKER_NAME,
							app.topLevelClass);
			return builder.build();
		}

		@Override
		public Configuration getDynamicConfiguration() {
			Configuration.Builder builder = Configuration.builder();

			Map<Integer, Integer> coresPerMachine = new HashMap<>();
			for (Entry<Integer, List<Set<Worker<?, ?>>>> machine : app.partitionsMachineMap
					.entrySet()) {
				coresPerMachine
						.put(machine.getKey(), machine.getValue().size());
			}

			PartitionParameter.Builder partParam = PartitionParameter.builder(
					GlobalConstants.PARTITION, coresPerMachine);

			BlobFactory factory = new Interpreter.InterpreterBlobFactory();
			partParam.addBlobFactory(factory);

			app.blobtoMachineMap = new HashMap<>();

			for (Integer machineID : app.partitionsMachineMap.keySet()) {
				List<Set<Worker<?, ?>>> blobList = app.partitionsMachineMap
						.get(machineID);
				for (Set<Worker<?, ?>> blobWorkers : blobList) {
					// TODO: One core per blob. Need to change this.
					partParam.addBlob(machineID, 1, factory, blobWorkers);

					// TODO: Temp fix to build.
					Token t = Utils.getblobID(blobWorkers);
					app.blobtoMachineMap.put(t, machineID);
				}
			}

			builder.addParameter(partParam.build());
			if (app.blobConfiguration != null)
				builder.addSubconfiguration("blobConfigs",
						app.blobConfiguration);
			return builder.build();
		}
	}
}
