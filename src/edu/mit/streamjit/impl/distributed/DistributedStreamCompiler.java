package edu.mit.streamjit.impl.distributed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Input.ManualInput;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.InputBufferFactory;
import edu.mit.streamjit.impl.common.OutputBufferFactory;
import edu.mit.streamjit.impl.common.TimeLogger;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.common.drainer.AbstractDrainer;
import edu.mit.streamjit.impl.concurrent.ConcurrentStreamCompiler;
import edu.mit.streamjit.impl.distributed.HeadChannel.HeadBuffer;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.impl.distributed.runtimer.CommunicationManager.CommunicationType;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;
import edu.mit.streamjit.impl.distributed.runtimer.DistributedDrainer;
import edu.mit.streamjit.impl.distributed.runtimer.OnlineTuner;
import edu.mit.streamjit.partitioner.HorizontalPartitioner;
import edu.mit.streamjit.partitioner.Partitioner;
import edu.mit.streamjit.util.ConfigurationUtils;

/**
 * 
 * The OneToOneElement that is asked to compile by this {@link Compiler} must be
 * unique. Compilation will fail if default subtypes of the
 * {@link OneToOneElement}s such as {@link Pipeline}, {@link Splitjoin} and etc
 * are be passed.
 * <p>
 * TODO: {@link DistributedStreamCompiler} must work with 1 {@link StreamNode}
 * as well. In that case, it should behave like a
 * {@link ConcurrentStreamCompiler}.
 * </p>
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 6, 2013
 */
public class DistributedStreamCompiler implements StreamCompiler {

	/**
	 * Configuration from Opentuner.
	 */
	private Configuration cfg;

	/**
	 * Total number of nodes including controller node.
	 */
	private int noOfnodes;

	/**
	 * Run the whole application on the controller node. No distributions. See
	 * {@link #DistributedStreamCompiler(int, Configuration)}
	 */
	public DistributedStreamCompiler() {
		this(1, null);
	}

	/**
	 * See {@link #DistributedStreamCompiler(int, Configuration)}. As no
	 * configuration is passed, tuner will activated to tune for better
	 * configuration.
	 */
	public DistributedStreamCompiler(int noOfnodes) {
		this(noOfnodes, null);
	}

	/**
	 * Run the application with the passed configuration. Pass null if the
	 * intention is to tune the application.
	 * 
	 * @param noOfnodes
	 *            : Total number of nodes the stream application intended to run
	 *            including the controller node. If it is 1 then it means the
	 *            whole stream application is supposed to run on controller.
	 * @param cfg
	 *            Run the application with the passed {@link Configuration}. If
	 *            it is null, tuner will be activated to tune for better
	 *            configuration.
	 */
	public DistributedStreamCompiler(int noOfnodes, Configuration cfg) {
		if (noOfnodes < 1)
			throw new IllegalArgumentException("noOfnodes must be 1 or greater");
		if (GlobalConstants.singleNodeOnline) {
			System.out
					.println("Flag GlobalConstants.singleNodeOnline is enabled."
							+ " noOfNodes passed as compiler argument has no effect");
			this.noOfnodes = 1;
		} else
			this.noOfnodes = noOfnodes;

		this.cfg = cfg;
	}

	public <I, O> CompiledStream compile(OneToOneElement<I, O> stream,
			Input<I> input, Output<O> output) {
		StreamJitApp<I, O> app = new StreamJitApp<>(stream);
		Controller controller = establishController();

		PartitionManager partitionManager = new HotSpotTuning(app);
		ConfigurationManager cfgManager = new ConfigurationManager(app,
				partitionManager);
		ConnectionManager conManager = new ConnectionManager.BlockingTCPNoParams(
				controller.controllerNodeID);
		setConfiguration(controller, app, partitionManager, conManager,
				cfgManager);

		TimeLogger logger = new TimeLoggers.FileTimeLogger(app.name);
		StreamJitAppManager manager = new StreamJitAppManager(controller, app,
				partitionManager, conManager, logger);
		final AbstractDrainer drainer = new DistributedDrainer(app, logger,
				manager);
		drainer.setBlobGraph(app.blobGraph);

		boolean needTermination = setBufferMap(input, output, drainer, app);

		manager.reconfigure(1);
		CompiledStream cs = new DistributedCompiledStream(drainer);

		if (GlobalConstants.tune > 0 && this.cfg != null) {
			OnlineTuner tuner = new OnlineTuner(drainer, manager, app,
					cfgManager, logger, needTermination);
			new Thread(tuner, "OnlineTuner").start();
		}
		return cs;
	}

	private <I, O> Configuration cfgFromFile(StreamJitApp<I, O> app,
			Controller controller, Configuration defaultCfg) {
		Configuration cfg1 = ConfigurationUtils.readConfiguration(app.name,
				null);
		if (cfg1 == null) {
			controller.closeAll();
			throw new IllegalConfigurationException();
		} else if (!verifyCfg(defaultCfg, cfg1)) {
			System.err
					.println("Reading the configuration from configuration file");
			System.err
					.println("No matching between parameters in the read "
							+ "configuration and parameters in the default configuration");
			controller.closeAll();
			throw new IllegalConfigurationException();
		}
		return cfg1;
	}

	private Controller establishController() {
		Map<CommunicationType, Integer> conTypeCount = new HashMap<>();

		if (this.noOfnodes == 1)
			conTypeCount.put(CommunicationType.LOCAL, 1);
		else
			conTypeCount.put(CommunicationType.TCP, this.noOfnodes - 1);
		Controller controller = new Controller();
		controller.connect(conTypeCount);
		return controller;
	}

	private <I, O> Map<Integer, List<Set<Worker<?, ?>>>> getMachineWorkerMap(
			Integer[] machineIds, OneToOneElement<I, O> stream,
			Worker<I, ?> source, Worker<?, O> sink) {
		int totalCores = machineIds.length;

		Partitioner<I, O> horzPartitioner = new HorizontalPartitioner<>();
		List<Set<Worker<?, ?>>> partitionList = horzPartitioner
				.partitionEqually(stream, source, sink, totalCores);

		Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap = new HashMap<Integer, List<Set<Worker<?, ?>>>>();
		for (Integer machineID : machineIds) {
			partitionsMachineMap.put(machineID,
					new ArrayList<Set<Worker<?, ?>>>());
		}

		int index = 0;
		while (index < partitionList.size()) {
			for (Integer machineID : partitionsMachineMap.keySet()) {
				if (!(index < partitionList.size()))
					break;
				partitionsMachineMap.get(machineID).add(
						partitionList.get(index++));
			}
		}
		return partitionsMachineMap;
	}

	private <I, O> void manualPartition(StreamJitApp<I, O> app) {
		Integer[] machineIds = new Integer[this.noOfnodes - 1];
		for (int i = 0; i < machineIds.length; i++) {
			machineIds[i] = i + 1;
		}
		Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap = getMachineWorkerMap(
				machineIds, app.streamGraph, app.source, app.sink);
		app.newPartitionMap(partitionsMachineMap);
	}

	/**
	 * Sets head and tail buffers.
	 */
	private <I, O> boolean setBufferMap(Input<I> input, Output<O> output,
			final AbstractDrainer drainer, StreamJitApp<I, O> app) {
		// TODO: derive a algorithm to find good buffer size and use here.
		Buffer head = InputBufferFactory.unwrap(input).createReadableBuffer(
				10000);
		Buffer tail = OutputBufferFactory.unwrap(output).createWritableBuffer(
				10000);

		boolean needTermination;

		if (input instanceof ManualInput) {
			needTermination = false;
			InputBufferFactory
					.setManualInputDelegate(
							(ManualInput<I>) input,
							new InputBufferFactory.AbstractManualInputDelegate<I>(
									head) {
								@Override
								public void drain() {
									// drainer.startDraining(2);
									drainer.drainFinal(false);
								}
							});
		} else {
			needTermination = true;
			head = new HeadBuffer(head, drainer);
		}

		ImmutableMap.Builder<Token, Buffer> bufferMapBuilder = ImmutableMap
				.<Token, Buffer> builder();

		bufferMapBuilder.put(Token.createOverallInputToken(app.source), head);
		bufferMapBuilder.put(Token.createOverallOutputToken(app.sink), tail);

		app.bufferMap = bufferMapBuilder.build();
		return needTermination;
	}

	private <I, O> void setConfiguration(Controller controller,
			StreamJitApp<I, O> app, PartitionManager partitionManager,
			ConnectionManager conManager, ConfigurationManager cfgManager) {
		BlobFactory bf = new DistributedBlobFactory(partitionManager,
				conManager, Math.max(noOfnodes - 1, 1));
		Configuration defaultCfg = bf.getDefaultConfiguration(Workers
				.getAllWorkersInGraph(app.source));

		if (this.cfg != null) {
			if (!verifyCfg(defaultCfg, this.cfg)) {
				System.err
						.println("No matching between parameters in the passed "
								+ "configuration and parameters in the default configuration");
				controller.closeAll();
				throw new IllegalConfigurationException();
			}
		} else if (GlobalConstants.tune == 0) {
			this.cfg = cfgFromFile(app, controller, defaultCfg);
		} else
			this.cfg = defaultCfg;

		cfgManager.newConfiguration(this.cfg);
	}

	private <I, O> boolean verifyCfg(Configuration defaultCfg, Configuration cfg) {
		if (defaultCfg.getParametersMap().keySet()
				.equals(cfg.getParametersMap().keySet()))
			return true;
		return false;
	}

	private static class DistributedCompiledStream implements CompiledStream {

		AbstractDrainer drainer;

		public DistributedCompiledStream(AbstractDrainer drainer) {
			this.drainer = drainer;
		}

		@Override
		public void awaitDrained() throws InterruptedException {
			drainer.awaitDrained();

		}

		@Override
		public void awaitDrained(long timeout, TimeUnit unit)
				throws InterruptedException, TimeoutException {
			drainer.awaitDrained(timeout, unit);
		}

		@Override
		public boolean isDrained() {
			return drainer.isDrained();
		}
	}

	private class IllegalConfigurationException extends RuntimeException {

		private static final long serialVersionUID = 1L;

		private static final String tag = "IllegalConfigurationException";

		private IllegalConfigurationException() {
			super(tag);
		}

		private IllegalConfigurationException(String msg) {
			super(String.format("%s : %s", tag, msg));
		}
	}
}