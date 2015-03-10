package edu.mit.streamjit.tuner;

import edu.mit.streamjit.impl.common.TimeLogger;
import edu.mit.streamjit.impl.common.drainer.AbstractDrainer;
import edu.mit.streamjit.impl.distributed.ConfigurationManager;
import edu.mit.streamjit.impl.distributed.StreamJitApp;
import edu.mit.streamjit.impl.distributed.StreamJitAppManager;
import edu.mit.streamjit.tuner.MethodTimeLogger.FileMethodTimeLogger;

/**
 * Re-factored the {@link OnlineTuner} and moved all streamjit app
 * reconfiguration related methods to this new class.
 * 
 * @author sumanan
 * @since 10 Mar, 2015
 */
public class Reconfigurer {

	final AbstractDrainer drainer;
	final StreamJitAppManager manager;
	final StreamJitApp<?, ?> app;
	final ConfigurationManager cfgManager;
	final TimeLogger logger;
	final ConfigurationPrognosticator prognosticator;
	final MethodTimeLogger mLogger;

	public Reconfigurer(AbstractDrainer drainer, StreamJitAppManager manager,
			StreamJitApp<?, ?> app, ConfigurationManager cfgManager,
			TimeLogger logger) {
		this.drainer = drainer;
		this.manager = manager;
		this.app = app;
		this.cfgManager = cfgManager;
		this.logger = logger;
		this.prognosticator = new GraphPropertyPrognosticator(app);
		this.mLogger = new FileMethodTimeLogger(app.name);
	}
}