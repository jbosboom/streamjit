package edu.mit.streamjit.impl.distributed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Input.ManualInput;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Portal;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StreamCompilationFailedException;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.AbstractDrainer;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.common.InputBufferFactory;
import edu.mit.streamjit.impl.common.MessageConstraint;
import edu.mit.streamjit.impl.common.OutputBufferFactory;
import edu.mit.streamjit.impl.common.Portals;
import edu.mit.streamjit.impl.common.VerifyStreamGraph;
import edu.mit.streamjit.impl.common.Workers;
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
import edu.mit.streamjit.util.Pair;

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
		Pair<Worker<I, ?>, Worker<?, O>> srcSink = visit(stream);
		Controller controller = establishController();
		StreamJitApp app = new StreamJitApp(stream, srcSink.first,
				srcSink.second);
		ConfigurationManager cfgManager = new HotSpotTuning(app);
		ConnectionManager conManager = new ConnectionManager.BlockingTCPNoParams(
				controller.controllerNodeID);
		setConfiguration(controller, srcSink, stream, app, cfgManager,
				conManager);

		StreamJitAppManager manager = new StreamJitAppManager(controller, app,
				cfgManager, conManager);
		final AbstractDrainer drainer = new DistributedDrainer(manager);
		drainer.setBlobGraph(app.blobGraph);

		boolean needTermination = setBufferMap(input, output, drainer, app,
				srcSink);
		setConstrains(srcSink, app);

		manager.reconfigure(1);
		CompiledStream cs = new DistributedCompiledStream(drainer);

		if (GlobalConstants.tune > 0 && this.cfg != null) {
			OnlineTuner tuner = new OnlineTuner(drainer, manager, app,
					cfgManager, needTermination);
			new Thread(tuner, "OnlineTuner").start();
		}
		return cs;
	}

	private <I, O> Configuration cfgFromFile(OneToOneElement<I, O> stream,
			Controller controller, Configuration defaultCfg) {
		Configuration cfg1 = ConfigurationUtils.readConfiguration(String
				.format("%s.cfg", stream.getClass().getSimpleName()));
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

	/**
	 * TODO: Need to check for other default subtypes of {@link OneToOneElement}
	 * s. Now only checks for first generation children.
	 * 
	 * @param stream
	 * @throws StreamCompilationFailedException
	 *             if stream is default subtype of OneToOneElement
	 */
	private <I, O> void checkforDefaultOneToOneElement(
			OneToOneElement<I, O> stream) {

		if (stream.getClass() == Pipeline.class
				|| stream.getClass() == Splitjoin.class
				|| stream.getClass() == Filter.class) {
			throw new StreamCompilationFailedException(
					"Default subtypes of OneToOneElement are not accepted for"
							+ " compilation by this compiler. OneToOneElement"
							+ " that passed should be unique");
		}
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

	private <I, O> void manualPartition(
			Pair<Worker<I, ?>, Worker<?, O>> srcSink,
			OneToOneElement<I, O> stream, StreamJitApp app) {
		Integer[] machineIds = new Integer[this.noOfnodes - 1];
		for (int i = 0; i < machineIds.length; i++) {
			machineIds[i] = i + 1;
		}
		Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap = getMachineWorkerMap(
				machineIds, stream, srcSink.first, srcSink.second);
		app.newPartitionMap(partitionsMachineMap);
	}

	/**
	 * Sets head and tail buffers.
	 */
	private <I, O> boolean setBufferMap(Input<I> input, Output<O> output,
			final AbstractDrainer drainer, StreamJitApp app,
			Pair<Worker<I, ?>, Worker<?, O>> srcSink) {
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
									drainer.startDraining(2);
								}
							});
		} else {
			needTermination = true;
			head = new HeadBuffer(head, drainer);
		}

		ImmutableMap.Builder<Token, Buffer> bufferMapBuilder = ImmutableMap
				.<Token, Buffer> builder();

		bufferMapBuilder
				.put(Token.createOverallInputToken(srcSink.first), head);
		bufferMapBuilder.put(Token.createOverallOutputToken(srcSink.second),
				tail);

		app.bufferMap = bufferMapBuilder.build();
		return needTermination;
	}

	private <I, O> void setConfiguration(Controller controller,
			Pair<Worker<I, ?>, Worker<?, O>> srcSink,
			OneToOneElement<I, O> stream, StreamJitApp app,
			ConfigurationManager cfgManager, ConnectionManager conManager) {
		BlobFactory bf = new DistributedBlobFactory(cfgManager, conManager,
				Math.max(noOfnodes - 1, 1));
		Configuration defaultCfg = bf.getDefaultConfiguration(Workers
				.getAllWorkersInGraph(srcSink.first));

		if (this.cfg != null) {
			if (!verifyCfg(defaultCfg, this.cfg)) {
				System.err
						.println("No matching between parameters in the passed "
								+ "configuration and parameters in the default configuration");
				controller.closeAll();
				throw new IllegalConfigurationException();
			}
		} else if (GlobalConstants.tune == 0) {
			this.cfg = cfgFromFile(stream, controller, defaultCfg);
		} else
			this.cfg = defaultCfg;

		cfgManager.newConfiguration(this.cfg);
	}

	private <I, O> void setConstrains(Pair<Worker<I, ?>, Worker<?, O>> srcSink,
			StreamJitApp app) {
		// TODO: Copied form DebugStreamCompiler. Need to be verified for this
		// context.
		List<MessageConstraint> constraints = MessageConstraint
				.findConstraints(srcSink.first);
		Set<Portal<?>> portals = new HashSet<>();
		for (MessageConstraint mc : constraints)
			portals.add(mc.getPortal());
		for (Portal<?> portal : portals)
			Portals.setConstraints(portal, constraints);
		app.constraints = constraints;
	}

	private <I, O> boolean verifyCfg(Configuration defaultCfg, Configuration cfg) {
		if (defaultCfg.getParametersMap().keySet()
				.equals(cfg.getParametersMap().keySet()))
			return true;
		return false;
	}

	private <I, O> Pair<Worker<I, ?>, Worker<?, O>> visit(
			OneToOneElement<I, O> stream) {
		checkforDefaultOneToOneElement(stream);
		ConnectWorkersVisitor primitiveConnector = new ConnectWorkersVisitor();
		stream.visit(primitiveConnector);
		Worker<I, ?> source = (Worker<I, ?>) primitiveConnector.getSource();
		Worker<?, O> sink = (Worker<?, O>) primitiveConnector.getSink();

		VerifyStreamGraph verifier = new VerifyStreamGraph();
		stream.visit(verifier);
		return new Pair<Worker<I, ?>, Worker<?, O>>(source, sink);
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