package edu.mit.streamjit.tuner;

import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.TimeLogger;
import edu.mit.streamjit.impl.common.drainer.AbstractDrainer;
import edu.mit.streamjit.impl.distributed.ConfigurationManager;
import edu.mit.streamjit.impl.distributed.StreamJitApp;
import edu.mit.streamjit.impl.distributed.StreamJitAppManager;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.tuner.MethodTimeLogger.FileMethodTimeLogger;
import edu.mit.streamjit.util.Pair;

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

	/**
	 * TODO: Split this method into two methods, 1.reconfigure(),
	 * 2.getFixedOutputTime().
	 * 
	 * @param cfgJson
	 * @param round
	 * @return if ret.first == false, then no more tuning. ret.second = running
	 *         time in milliseconds. ret.second may be a negative value if the
	 *         reconfiguration is unsuccessful or a timeout is occurred.
	 *         Meanings of the negative values are follows
	 *         <ol>
	 *         <li>-1: Timeout has occurred.
	 *         <li>-2: Invalid configuration.
	 *         <li>-3: {@link ConfigurationPrognosticator} has rejected the
	 *         configuration.
	 *         <li>-4: Draining failed. Another draining is in progress.
	 *         <li>-5: Reconfiguration has failed at {@link StreamNode} side.
	 *         E.g., Compilation error.
	 *         <li>-6: Misc problems.
	 */
	public Pair<Boolean, Long> reconfigure(Configuration config, long timeout) {
		long time;

		if (manager.getStatus() == AppStatus.STOPPED)
			return new Pair<Boolean, Long>(false, 0l);

		mLogger.bCfgManagerNewcfg();
		boolean validCfg = cfgManager.newConfiguration(config);
		mLogger.eCfgManagerNewcfg();
		if (!validCfg)
			return new Pair<Boolean, Long>(true, -2l);

		mLogger.bPrognosticate();
		boolean prog = prognosticator.prognosticate(config);
		mLogger.ePrognosticate();
		if (!prog)
			return new Pair<Boolean, Long>(true, -3l);

		try {
			mLogger.bIntermediateDraining();
			boolean intermediateDraining = intermediateDraining();
			mLogger.eIntermediateDraining();
			if (!intermediateDraining)
				return new Pair<Boolean, Long>(false, -4l);

			drainer.setBlobGraph(app.blobGraph);
			int multiplier = getMultiplier(config);
			mLogger.bManagerReconfigure();
			boolean reconfigure = manager.reconfigure(multiplier);
			mLogger.eManagerReconfigure();
			if (reconfigure) {
				// TODO: need to check the manager's status before passing the
				// time. Exceptions, final drain, etc may causes app to stop
				// executing.
				mLogger.bGetFixedOutputTime();
				time = manager.getFixedOutputTime(timeout);
				mLogger.eGetFixedOutputTime();
				logger.logRunTime(time);
			} else {
				time = -5l;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			System.err
					.println("Couldn't compile the stream graph with this configuration");
			time = -6l;
		}
		return new Pair<Boolean, Long>(true, time);
	}

	/**
	 * Performs intermediate draining.
	 * 
	 * @return <code>true</code> iff the draining is success or the application
	 *         is not running currently.
	 * @throws InterruptedException
	 */
	private boolean intermediateDraining() throws InterruptedException {
		if (manager.isRunning()) {
			return drainer.drainIntermediate();
		} else
			return true;
	}

	private int getMultiplier(Configuration config) {
		int multiplier = 50;
		IntParameter mulParam = config.getParameter("multiplier",
				IntParameter.class);
		if (mulParam != null)
			multiplier = mulParam.getValue();
		System.err.println("Reconfiguring...multiplier = " + multiplier);
		return multiplier;
	}

}