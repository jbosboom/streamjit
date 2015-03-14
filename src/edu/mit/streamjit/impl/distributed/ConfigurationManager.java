/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.impl.distributed;

import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.mit.streamjit.api.StreamCompilationFailedException;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.drainer.BlobGraph;
import edu.mit.streamjit.tuner.OnlineTuner;

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
