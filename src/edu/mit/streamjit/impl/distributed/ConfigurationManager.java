package edu.mit.streamjit.impl.distributed;

import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.mit.streamjit.api.StreamCompilationFailedException;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.AbstractDrainer.BlobGraph;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.distributed.runtimer.OnlineTuner;

public class ConfigurationManager {

	private final StreamJitApp<?, ?> app;

	private final PartitionManager partitionManager;

	public ConfigurationManager(StreamJitApp<?, ?> app,
			PartitionManager partitionManager) {
		this.app = app;
		this.partitionManager = partitionManager;
	}

	/**
	 * This method may be called to by the {@link OnlineTuner} to interpret a
	 * new configuration and execute the steramjit app with the new
	 * configuration.
	 * <p>
	 * Builds partitionsMachineMap and {@link BlobGraph} from the new
	 * Configuration, and verifies for any cycles among blobs. If it is a valid
	 * configuration, (i.e., no cycles among the blobs), then {@link #app}
	 * object's member variables {@link StreamJitApp#blobConfiguration},
	 * {@link StreamJitApp#blobGraph} and
	 * {@link StreamJitApp#partitionsMachineMap} will be assigned according to
	 * reflect the new configuration, no changes otherwise.
	 * 
	 * @param config
	 *            configuration from {@link OnlineTuner}.
	 * @return true iff valid configuration is passed.
	 */
	public boolean newConfiguration(Configuration config) {
		// for (Parameter p : config.getParametersMap().values()) {
		// if (p instanceof IntParameter) {
		// IntParameter ip = (IntParameter) p;
		// System.out.println(ip.getName() + " - " + ip.getValue());
		// } else if (p instanceof SwitchParameter<?>) {
		// SwitchParameter<?> sp = (SwitchParameter<?>) p;
		// System.out.println(sp.getName() + " - " + sp.getValue());
		// } else
		// System.out.println(p.getName() + " - Unknown type");
		// }

		Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap = partitionManager
				.partitionMap(config);
		try {
			app.verifyConfiguration(partitionsMachineMap);
		} catch (StreamCompilationFailedException ex) {
			return false;
		}
		app.setConfiguration(config);
		return true;
	}
}
