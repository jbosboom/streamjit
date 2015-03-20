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
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.TimeLogger;
import edu.mit.streamjit.impl.common.drainer.AbstractDrainer;
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
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionProvider;
import edu.mit.streamjit.impl.distributed.common.Error.ErrorProcessor;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.MiscCtrlElements.NewConInfo;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.Drained;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.SNDrainProcessor;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.SNDrainedData;
import edu.mit.streamjit.impl.distributed.common.SNException;
import edu.mit.streamjit.impl.distributed.common.SNException.AddressBindException;
import edu.mit.streamjit.impl.distributed.common.SNException.SNExceptionProcessor;
import edu.mit.streamjit.impl.distributed.common.SNTimeInfo.SNTimeInfoProcessor;
import edu.mit.streamjit.impl.distributed.common.SNTimeInfoProcessorImpl;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.impl.distributed.profiler.MasterProfiler;
import edu.mit.streamjit.impl.distributed.profiler.ProfilerCommand;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;
import edu.mit.streamjit.util.ConfigurationUtils;

public class StreamJitAppManager {

	private final StreamJitApp<?, ?> app;

	private AppStatusProcessorImpl apStsPro = null;

	private Map<Token, ConnectionInfo> conInfoMap;

	private final ConnectionManager conManager;

	private final Controller controller;

	private SNDrainProcessorImpl dp = null;

	private ErrorProcessor ep = null;

	private SNExceptionProcessorImpl exP = null;

	private final MasterProfiler profiler;

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

	private final TimeLogger logger;

	private final SNTimeInfoProcessor timeInfoProcessor;

	public StreamJitAppManager(Controller controller, StreamJitApp<?, ?> app,
			ConnectionManager conManager, TimeLogger logger) {
		this.controller = controller;
		this.app = app;
		this.conManager = conManager;
		this.logger = logger;
		this.timeInfoProcessor = new SNTimeInfoProcessorImpl(logger);
		this.status = AppStatus.NOT_STARTED;
		this.exP = new SNExceptionProcessorImpl();
		this.ep = new ErrorProcessorImpl();
		this.apStsPro = new AppStatusProcessorImpl(controller.getAllNodeIDs()
				.size());
		controller.registerManager(this);
		controller.newApp(app.getStaticConfiguration()); // TODO: Find a
															// good calling
															// place.
		isRunning = false;

		headToken = Token.createOverallInputToken(app.source);
		tailToken = Token.createOverallOutputToken(app.sink);
		profiler = setupProfiler();
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
			if (Options.useDrainData)
				if (isFinal)
					tailChannel.stop(DrainType.FINAL);
				else
					tailChannel.stop(DrainType.INTERMEDIATE);
			else
				tailChannel.stop(DrainType.DISCARD);

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
		if (sw != null && sw.isRunning()) {
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

	public SNTimeInfoProcessor timeInfoProcessor() {
		return timeInfoProcessor;
	}

	public long getFixedOutputTime(long timeout) throws InterruptedException {
		long time = tailChannel.getFixedOutputTime(timeout);
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
		Configuration.Builder builder = Configuration.builder(app
				.getDynamicConfiguration());

		conInfoMap = conManager.conInfoMap(app.getConfiguration(),
				app.partitionsMachineMap, app.source, app.sink);

		builder.putExtraData(GlobalConstants.CONINFOMAP, conInfoMap);

		Configuration cfg = builder.build();
		String jsonStirng = cfg.toJson();

		ImmutableMap<Integer, DrainData> drainDataMap = app.getDrainData();

		logger.compilationStarted();
		for (int nodeID : controller.getAllNodeIDs()) {
			ConfigurationString json = new ConfigurationString(jsonStirng,
					ConfigType.DYNAMIC, drainDataMap.get(nodeID));
			controller.send(nodeID, json);
		}

		setupHeadTail(conInfoMap, app.bufferMap, multiplier);

		boolean isCompiled = apStsPro.waitForCompilation();
		logger.compilationFinished(isCompiled, "");

		if (isCompiled) {
			start();
			isRunning = true;
		} else {
			isRunning = false;
		}

		if (profiler != null) {
			String cfgPrefix = ConfigurationUtils.getConfigPrefix(app
					.getConfiguration());
			profiler.logger().newConfiguration(cfgPrefix);
		}

		System.out.println("StraemJit app is running...");
		Utils.printMemoryStatus();
		return isRunning;
	}
	public void setDrainer(AbstractDrainer drainer) {
		assert dp == null : "SNDrainProcessor has already been set";
		this.dp = new SNDrainProcessorImpl(drainer);
	}

	public void stop() {
		this.status = AppStatus.STOPPED;
		tailChannel.reset();
		controller.closeAll();
		dp.drainer.stop();
	}

	private void reset() {
		exP.exConInfos = new HashSet<>();
		apStsPro.reset();
	}

	private MasterProfiler setupProfiler() {
		MasterProfiler p = null;
		if (Options.needProfiler) {
			p = new MasterProfiler(app.name);
			controller.sendToAll(ProfilerCommand.START);
		}
		return p;
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

		int skipCount = Math.max(Options.outputCount, multiplier * 5);
		tailChannel = tailChannel(bufferMap.get(tailToken), tailconInfo,
				skipCount);
	}

	private TailChannel tailChannel(Buffer buffer, ConnectionInfo conInfo,
			int skipCount) {
		String appName = app.name;
		int steadyCount = Options.outputCount;
		int debugLevel = 0;
		String bufferTokenName = "tailChannel - " + tailToken.toString();
		ConnectionProvider conProvider = controller.getConProvider();
		String cfgPrefix = ConfigurationUtils.getConfigPrefix(app
				.getConfiguration());
		switch (Options.tailChannel) {
			case 1 :
				return new TailChannels.BlockingTailChannel1(buffer,
						conProvider, conInfo, bufferTokenName, debugLevel,
						skipCount, steadyCount, appName, cfgPrefix);
			default :
				return new TailChannels.BlockingTailChannel2(buffer,
						conProvider, conInfo, bufferTokenName, debugLevel,
						skipCount, steadyCount, appName, cfgPrefix);
		}
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
			tailThread = new Thread(tailChannel.getRunnable(),
					tailChannel.name());
			tailThread.start();
		}
	}

	public MasterProfiler getProfiler() {
		return profiler;
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
			tailChannel.reset();
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
		public void process(SNDrainedData snDrainedData) {
			if (Options.useDrainData)
				drainer.newSNDrainData(snDrainedData);
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
