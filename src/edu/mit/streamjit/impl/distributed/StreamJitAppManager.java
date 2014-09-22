package edu.mit.streamjit.impl.distributed;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.AbstractDrainer;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.AppStatus.AppStatusProcessor;
import edu.mit.streamjit.impl.distributed.common.AsyncTCPConnection.AsyncTCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.DrainType;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageElement;
import edu.mit.streamjit.impl.distributed.common.Command;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString.ConfigurationProcessor.ConfigType;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.Error.ErrorProcessor;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.MiscCtrlElements.NewConInfo;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.Drained;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.DrainedData;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.SNDrainProcessor;
import edu.mit.streamjit.impl.distributed.common.SNException;
import edu.mit.streamjit.impl.distributed.common.SNException.AddressBindException;
import edu.mit.streamjit.impl.distributed.common.SNException.SNExceptionProcessor;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;

public class StreamJitAppManager {

	private final StreamJitApp app;

	private AppStatusProcessorImpl apStsPro = null;

	private final ConfigurationManager cfgManager;

	private Map<Token, ConnectionInfo> conInfoMap;

	private final ConnectionManager conManager;

	private final Controller controller;

	private SNDrainProcessorImpl dp = null;

	private ErrorProcessor ep = null;

	private SNExceptionProcessorImpl exP = null;

	/**
	 * A {@link BoundaryOutputChannel} for the head of the stream graph. If the
	 * first {@link Worker} happened to fall outside the {@link Controller}, we
	 * need to push the {@link CompiledStream}.offer() data to the first
	 * {@link Worker} of the streamgraph.
	 */
	private BoundaryOutputChannel headChannel;

	private Thread headThread;

	private final Token headToken;

	private boolean isRunning;

	private volatile AppStatus status;

	/**
	 * A {@link BoundaryInputChannel} for the tail of the whole stream graph. If
	 * the sink {@link Worker} happened to fall outside the {@link Controller},
	 * we need to pull the sink's output in to the {@link Controller} in order
	 * to make {@link CompiledStream} .pull() to work.
	 */
	private TailChannel tailChannel;

	private Thread tailThread;

	private final Token tailToken;

	/**
	 * [2014-03-15] Just to measure the draining time
	 */
	AtomicReference<Stopwatch> stopwatchRef = new AtomicReference<>();

	public StreamJitAppManager(Controller controller, StreamJitApp app,
			ConfigurationManager cfgManager, ConnectionManager conManager) {
		this.controller = controller;
		this.app = app;
		this.cfgManager = cfgManager;
		this.conManager = conManager;
		this.status = AppStatus.NOT_STARTED;
		this.exP = new SNExceptionProcessorImpl();
		this.ep = new ErrorProcessorImpl();
		this.apStsPro = new AppStatusProcessorImpl(controller.getAllNodeIDs()
				.size());
		controller.registerManager(this);
		controller.newApp(cfgManager.getStaticConfiguration()); // TODO: Find a
																// good calling
																// place.
		isRunning = false;

		headToken = Token.createOverallInputToken(app.source);
		tailToken = Token.createOverallOutputToken(app.sink);
	}

	public AppStatusProcessor appStatusProcessor() {
		return apStsPro;
	}

	public void drain(Token blobID, DrainType drainType) {
		// System.out.println("Drain requested to blob " + blobID);
		if (!app.blobtoMachineMap.containsKey(blobID))
			throw new IllegalArgumentException(blobID
					+ " not found in the blobtoMachineMap");
		int nodeID = app.blobtoMachineMap.get(blobID);
		controller.send(nodeID,
				new CTRLRDrainElement.DoDrain(blobID, drainType));
	}

	public void drainingFinished(boolean isFinal) {
		System.out.println("App Manager : Draining Finished...");

		if (headChannel != null) {
			try {
				headThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		if (tailChannel != null) {
			if (isFinal)
				tailChannel.stop(1);
			else if (GlobalConstants.useDrainData)
				tailChannel.stop(2);
			else
				tailChannel.stop(3);
			try {
				tailThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		if (isFinal)
			stop();

		isRunning = false;

		Stopwatch sw = stopwatchRef.get();
		if (sw != null) {
			sw.stop();
			long time = sw.elapsed(TimeUnit.MILLISECONDS);
			System.out.println("Draining time is " + time + " milli seconds");
		}
	}

	public void drainingStarted(boolean isFinal) {
		stopwatchRef.set(Stopwatch.createStarted());
		if (headChannel != null) {
			headChannel.stop(isFinal);
			// [2014-03-16] Moved to drainingFinished. In any case if headThread
			// blocked at tcp write, draining will also blocked.
			// try {
			// headThread.join();
			// } catch (InterruptedException e) {
			// e.printStackTrace();
			// }
		}
	}

	public SNDrainProcessor drainProcessor() {
		return dp;
	}

	public ErrorProcessor errorProcessor() {
		return ep;
	}

	public SNExceptionProcessor exceptionProcessor() {
		return exP;
	}

	public long getFixedOutputTime() throws InterruptedException {
		long time = tailChannel.getFixedOutputTime();
		if (apStsPro.error) {
			return -1l;
		}
		return time;
	}

	public AppStatus getStatus() {
		return status;
	}

	public boolean isRunning() {
		return isRunning;
	}

	public boolean reconfigure(int multiplier) {
		reset();
		Configuration.Builder builder = Configuration.builder(cfgManager
				.getDynamicConfiguration());

		conInfoMap = conManager.conInfoMap(app.blobConfiguration,
				app.partitionsMachineMap, app.source, app.sink);

		builder.putExtraData(GlobalConstants.CONINFOMAP, conInfoMap);

		Configuration cfg = builder.build();
		String jsonStirng = cfg.toJson();

		ImmutableMap<Integer, DrainData> drainDataMap = app.getDrainData();

		for (int nodeID : controller.getAllNodeIDs()) {
			ConfigurationString json = new ConfigurationString(jsonStirng,
					ConfigType.DYNAMIC, drainDataMap.get(nodeID));
			controller.send(nodeID, json);
		}

		setupHeadTail(conInfoMap, app.bufferMap, multiplier);

		boolean isCompiled = apStsPro.waitForCompilation();

		if (isCompiled) {
			start();
			isRunning = true;
		} else {
			isRunning = false;
		}

		long heapMaxSize = Runtime.getRuntime().maxMemory();
		long heapSize = Runtime.getRuntime().totalMemory();
		long heapFreeSize = Runtime.getRuntime().freeMemory();

		System.out.println("##############Controller######################");

		System.out.println("heapMaxSize = " + heapMaxSize / 1e6);
		System.out.println("heapSize = " + heapSize / 1e6);
		System.out.println("heapFreeSize = " + heapFreeSize / 1e6);
		System.out.println("StraemJit app is running...");
		System.out.println("##############################################");

		return isRunning;
	}

	public void setDrainer(AbstractDrainer drainer) {
		assert dp == null : "SNDrainProcessor has already been set";
		this.dp = new SNDrainProcessorImpl(drainer);
	}

	public void stop() {
		this.status = AppStatus.STOPPED;
		tailChannel.releaseAll();
		controller.closeAll();
		dp.drainer.stop();
	}

	private void reset() {
		exP.exConInfos = new HashSet<>();
		apStsPro.reset();
	}

	/**
	 * Setup the headchannel and tailchannel.
	 * 
	 * @param cfg
	 * @param bufferMap
	 */
	private void setupHeadTail(Map<Token, ConnectionInfo> conInfoMap,
			ImmutableMap<Token, Buffer> bufferMap, int multiplier) {

		ConnectionInfo headconInfo = conInfoMap.get(headToken);
		assert headconInfo != null : "No head connection info exists in conInfoMap";
		assert headconInfo.getSrcID() == controller.controllerNodeID
				|| headconInfo.getDstID() == controller.controllerNodeID : "Head channel should start from the controller. "
				+ headconInfo;

		if (!bufferMap.containsKey(headToken))
			throw new IllegalArgumentException(
					"No head buffer in the passed bufferMap.");

		if (headconInfo instanceof TCPConnectionInfo)
			headChannel = new HeadChannel.TCPHeadChannel(
					bufferMap.get(headToken), controller.getConProvider(),
					headconInfo, "headChannel - " + headToken.toString(), 0);
		else if (headconInfo instanceof AsyncTCPConnectionInfo)
			headChannel = new HeadChannel.AsyncHeadChannel(
					bufferMap.get(headToken), controller.getConProvider(),
					headconInfo, "headChannel - " + headToken.toString(), 0);
		else
			throw new IllegalStateException("Head ConnectionInfo doesn't match");

		ConnectionInfo tailconInfo = conInfoMap.get(tailToken);
		assert tailconInfo != null : "No tail connection info exists in conInfoMap";
		assert tailconInfo.getSrcID() == controller.controllerNodeID
				|| tailconInfo.getDstID() == controller.controllerNodeID : "Tail channel should ends at the controller. "
				+ tailconInfo;

		if (!bufferMap.containsKey(tailToken))
			throw new IllegalArgumentException(
					"No tail buffer in the passed bufferMap.");

		int skipCount = Math.max(GlobalConstants.outputCount, multiplier * 5);
		tailChannel = new TailChannel(bufferMap.get(tailToken),
				controller.getConProvider(), tailconInfo, "tailChannel - "
						+ tailToken.toString(), 0, skipCount,
				GlobalConstants.outputCount);
	}

	/**
	 * Start the execution of the StreamJit application.
	 */
	private void start() {
		if (isRunning)
			throw new IllegalStateException("Application is already running.");

		if (headChannel != null) {
			headThread = new Thread(headChannel.getRunnable(),
					headChannel.name());
			headThread.start();
		}

		controller.sendToAll(Command.START);

		if (tailChannel != null) {
			tailChannel.reset();
			tailThread = new Thread(tailChannel.getRunnable(),
					tailChannel.name());
			tailThread.start();
		}
	}

	/**
	 * {@link AppStatusProcessor} at {@link Controller} side.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since Aug 11, 2013
	 */
	private class AppStatusProcessorImpl implements AppStatusProcessor {

		private boolean compilationError;

		private CountDownLatch compileLatch;

		private volatile boolean error;

		private final int noOfnodes;

		private AppStatusProcessorImpl(int noOfnodes) {
			this.noOfnodes = noOfnodes;
		}

		@Override
		public void processCOMPILATION_ERROR() {
			System.err.println("Compilation error");
			this.compilationError = true;
			compileLatch.countDown();
		}

		@Override
		public void processCOMPILED() {
			compileLatch.countDown();
		}

		@Override
		public void processERROR() {
			this.error = true;
			// This will release the OpenTuner thread which is waiting for fixed
			// output.
			tailChannel.releaseAll();
		}

		@Override
		public void processNO_APP() {
		}

		@Override
		public void processNOT_STARTED() {
		}

		@Override
		public void processRUNNING() {
		}

		@Override
		public void processSTOPPED() {
		}

		private void reset() {
			compileLatch = new CountDownLatch(noOfnodes);
			this.compilationError = false;
			this.error = false;
		}

		private boolean waitForCompilation() {
			try {
				compileLatch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return !this.compilationError;
		}
	}

	/**
	 * {@link ErrorProcessor} at {@link Controller} side.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since Aug 11, 2013
	 */
	private class ErrorProcessorImpl implements ErrorProcessor {

		@Override
		public void processFILE_NOT_FOUND() {
			System.err
					.println("No application jar file in streamNode. Terminating...");
			stop();
		}

		@Override
		public void processWORKER_NOT_FOUND() {
			System.err
					.println("No top level class in the jar file. Terminating...");
			stop();
		}
	}

	/**
	 * {@link DrainProcessor} at {@link Controller} side.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since Aug 11, 2013
	 */
	private class SNDrainProcessorImpl implements SNDrainProcessor {

		AbstractDrainer drainer;

		public SNDrainProcessorImpl(AbstractDrainer drainer) {
			this.drainer = drainer;
		}

		@Override
		public void process(Drained drained) {
			drainer.drained(drained.blobID);
		}

		@Override
		public void process(DrainedData drainedData) {
			if (GlobalConstants.useDrainData)
				drainer.newDrainData(drainedData);
		}
	}

	private class SNExceptionProcessorImpl implements SNExceptionProcessor {

		private final Object abExLock = new Object();

		private Set<ConnectionInfo> exConInfos;

		private SNExceptionProcessorImpl() {
			exConInfos = new HashSet<>();
		}

		@Override
		public void process(AddressBindException abEx) {
			synchronized (abExLock) {
				if (exConInfos.contains(abEx.conInfo)) {
					System.out
							.println("AddressBindException : Already handled...");
					return;
				}

				Token t = null;
				for (Map.Entry<Token, ConnectionInfo> entry : conInfoMap
						.entrySet()) {
					if (abEx.conInfo.equals(entry.getValue())) {
						t = entry.getKey();
						break;
					}
				}

				if (t == null) {
					throw new IllegalArgumentException(
							"Illegal TCP connection - " + abEx.conInfo);
				}

				ConnectionInfo coninfo = conManager
						.replaceConInfo(abEx.conInfo);

				exConInfos.add(abEx.conInfo);

				CTRLRMessageElement msg = new NewConInfo(coninfo, t);
				controller.send(coninfo.getSrcID(), msg);
				controller.send(coninfo.getDstID(), msg);
			}
		}

		@Override
		public void process(SNException ex) {
		}
	}
}
