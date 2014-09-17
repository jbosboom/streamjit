package edu.mit.streamjit.impl.distributed.node;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Sets;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.Buffers;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannelManager.BoundaryInputChannelManager;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannelManager.BoundaryOutputChannelManager;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannelManager.InputChannelManager;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannelManager.OutputChannelManager;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.CTRLRDrainProcessor;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.DoDrain;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.DrainDataRequest;
import edu.mit.streamjit.impl.distributed.common.Command.CommandProcessor;
import edu.mit.streamjit.impl.distributed.common.Connection;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionProvider;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.DrainedData;
import edu.mit.streamjit.impl.distributed.common.SNMessageElement;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.impl.distributed.node.BufferManager.LocalBufferManager;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;

/**
 * {@link BlobsManagerImpl} responsible to run all {@link Blob}s those are
 * assigned to the {@link StreamNode}.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
public class BlobsManagerImpl implements BlobsManager {

	private Map<Token, BlobExecuter> blobExecuters;

	private final BufferManager bufferManager;

	private final CommandProcessor cmdProcessor;

	private final Map<Token, ConnectionInfo> conInfoMap;

	private final ConnectionProvider conProvider;

	private volatile DrainDeadLockHandler drainDeadLockHandler;

	private final CTRLRDrainProcessor drainProcessor;

	private MonitorBuffers monBufs;

	private final StreamNode streamNode;

	/**
	 * if true {@link DrainDeadLockHandler} will be used to unlock the draining
	 * time dead lock. Otherwise dynamic buffer will be used for local buffers
	 * to handled drain time data growth.
	 */
	private final boolean useDrainDeadLockHandler;

	public BlobsManagerImpl(ImmutableSet<Blob> blobSet,
			Map<Token, ConnectionInfo> conInfoMap, StreamNode streamNode,
			ConnectionProvider conProvider) {
		this.conInfoMap = conInfoMap;
		this.streamNode = streamNode;
		this.conProvider = conProvider;

		this.cmdProcessor = new CommandProcessorImpl();
		this.drainProcessor = new CTRLRDrainProcessorImpl();
		this.drainDeadLockHandler = null;
		this.useDrainDeadLockHandler = false;
		this.bufferManager = new LocalBufferManager(blobSet);

		bufferManager.initialise();
		if (bufferManager.isbufferSizesReady())
			createBEs(blobSet);
	}

	/**
	 * Drain the blob identified by the token.
	 */
	public void drain(Token blobID, boolean reqDrainData) {
		for (BlobExecuter be : blobExecuters.values()) {
			if (be.getBlobID().equals(blobID)) {
				be.doDrain(reqDrainData);
				return;
			}
		}
		throw new IllegalArgumentException(String.format(
				"No blob with blobID %s", blobID));
	}

	public CommandProcessor getCommandProcessor() {
		return cmdProcessor;
	}

	public CTRLRDrainProcessor getDrainProcessor() {
		return drainProcessor;
	}

	public void reqDrainedData(Set<Token> blobSet) {
		throw new UnsupportedOperationException(
				"Method reqDrainedData not implemented");
	}

	/**
	 * Start and execute the blobs. This function should be responsible to
	 * manage all CPU and I/O threads those are related to the {@link Blob}s.
	 */
	public void start() {
		for (BlobExecuter be : blobExecuters.values())
			be.startChannels();

		for (BlobExecuter be : blobExecuters.values())
			be.start();

		// if (monBufs == null) {
		// // System.out.println("Creating new MonitorBuffers");
		// monBufs = new MonitorBuffers();
		// monBufs.start();
		// } else
		// System.err
		// .println("Mon buffer is not null. Check the logic for bug");
	}

	/**
	 * Stop all {@link Blob}s if running. No effect if a {@link Blob} is already
	 * stopped.
	 */
	public void stop() {
		for (BlobExecuter be : blobExecuters.values())
			be.stop();

		if (monBufs != null)
			monBufs.stopMonitoring();

		if (drainDeadLockHandler != null)
			drainDeadLockHandler.stopit();
	}

	private void createBEs(ImmutableSet<Blob> blobSet) {
		assert bufferManager.isbufferSizesReady() : "Buffer sizes must be available to create BlobExecuters.";
		blobExecuters = new HashMap<>();
		Set<Token> locaTokens = bufferManager.localTokens();
		ImmutableMap<Token, Integer> bufferSizesMap = bufferManager
				.bufferSizes();
		for (Blob b : blobSet) {
			Token t = Utils.getBlobID(b);
			ImmutableMap<Token, BoundaryInputChannel> inputChannels = createInputChannels(
					Sets.difference(b.getInputs(), locaTokens), bufferSizesMap);
			ImmutableMap<Token, BoundaryOutputChannel> outputChannels = createOutputChannels(
					Sets.difference(b.getOutputs(), locaTokens), bufferSizesMap);
			blobExecuters.put(t, new BlobExecuter(t, b, inputChannels,
					outputChannels));
		}
	}

	private ImmutableMap<Token, BoundaryInputChannel> createInputChannels(
			Set<Token> inputTokens, ImmutableMap<Token, Integer> bufferMap) {
		ImmutableMap.Builder<Token, BoundaryInputChannel> inputChannelMap = new ImmutableMap.Builder<>();
		for (Token t : inputTokens) {
			ConnectionInfo conInfo = conInfoMap.get(t);
			inputChannelMap.put(t,
					conInfo.inputChannel(t, bufferMap.get(t), conProvider));
		}
		return inputChannelMap.build();
	}

	private ImmutableMap<Token, BoundaryOutputChannel> createOutputChannels(
			Set<Token> outputTokens, ImmutableMap<Token, Integer> bufferMap) {
		ImmutableMap.Builder<Token, BoundaryOutputChannel> outputChannelMap = new ImmutableMap.Builder<>();
		for (Token t : outputTokens) {
			ConnectionInfo conInfo = conInfoMap.get(t);
			outputChannelMap.put(t,
					conInfo.outputChannel(t, bufferMap.get(t), conProvider));
		}
		return outputChannelMap.build();
	}

	/**
	 * Just to added for debugging purpose.
	 */
	private synchronized void printDrainedStatus() {
		System.out.println("****************************************");
		for (BlobExecuter be : blobExecuters.values()) {
			switch (be.drainState) {
				case 0 :
					System.out.println(String.format("%s - No Drain Called",
							be.blobID));
					break;
				case 1 :
					System.out.println(String.format("%s - Drain Called",
							be.blobID));
					break;
				case 2 :
					System.out.println(String.format(
							"%s - Drain Passed to Interpreter", be.blobID));
					break;
				case 3 :
					System.out.println(String.format(
							"%s - Returned from Interpreter", be.blobID));
					break;
				case 4 :
					System.out.println(String.format(
							"%s - Draining Completed. All threads stopped.",
							be.blobID));
					break;
				case 5 :
					System.out.println(String.format(
							"%s - Processing Drain data", be.blobID));
					break;
				case 6 :
					System.out.println(String.format("%s - Draindata sent",
							be.blobID));
					break;
			}
		}
		System.out.println("****************************************");
	}

	private class BlobExecuter {

		private Blob blob;

		private final Token blobID;

		private Set<BlobThread2> blobThreads;

		/**
		 * Buffers for all input and output edges of the {@link #blob}.
		 */
		private ImmutableMap<Token, Buffer> bufferMap;

		/**
		 * This flag will be set to true if an exception thrown by the core code
		 * of the {@link Blob}. Any exception occurred in a blob's corecode will
		 * be informed to {@link Controller} to halt the application. See the
		 * {@link BlobThread2}.
		 */
		private AtomicBoolean crashed;

		private volatile int drainState;

		private final BoundaryInputChannelManager inChnlManager;

		private final BoundaryOutputChannelManager outChnlManager;

		private boolean reqDrainData;

		private BlobExecuter(Token t, Blob blob,
				ImmutableMap<Token, BoundaryInputChannel> inputChannels,
				ImmutableMap<Token, BoundaryOutputChannel> outputChannels) {
			this.crashed = new AtomicBoolean(false);
			this.blob = blob;
			this.blobThreads = new HashSet<>();
			assert blob.getInputs().containsAll(inputChannels.keySet());
			assert blob.getOutputs().containsAll(outputChannels.keySet());
			this.inChnlManager = new InputChannelManager(inputChannels);
			this.outChnlManager = new OutputChannelManager(outputChannels);

			String baseName = getName(blob);
			for (int i = 0; i < blob.getCoreCount(); i++) {
				String name = String.format("%s - %d", baseName, i);
				blobThreads
						.add(new BlobThread2(blob.getCoreCode(i), this, name));
			}

			if (blobThreads.size() < 1)
				throw new IllegalStateException("No blobs to execute");

			drainState = 0;
			this.blobID = t;
		}

		public Token getBlobID() {
			return blobID;
		}

		/**
		 * Gets buffer from {@link BoundaryChannel}s and builds bufferMap. The
		 * bufferMap will contain all input and output edges of the
		 * {@link #blob}.
		 * 
		 * Note that, Some {@link BoundaryChannel}s (e.g.,
		 * {@link AsyncOutputChannel}) create {@link Buffer}s after establishing
		 * {@link Connection} with other end. So this method must be called
		 * after establishing all IO connections.
		 * {@link InputChannelManager#waitToStart()} and
		 * {@link OutputChannelManager#waitToStart()} ensure that the IO
		 * connections are successfully established.
		 * 
		 * @return Buffer map which contains {@link Buffers} for all input and
		 *         output edges of the {@link #blob}.
		 */
		private ImmutableMap<Token, Buffer> buildBufferMap() {
			ImmutableMap.Builder<Token, Buffer> bufferMapBuilder = ImmutableMap
					.builder();
			ImmutableMap<Token, Buffer> localBufferMap = bufferManager
					.localBufferMap();
			ImmutableMap<Token, BoundaryInputChannel> inputChannels = inChnlManager
					.inputChannelsMap();
			ImmutableMap<Token, BoundaryOutputChannel> outputChannels = outChnlManager
					.outputChannelsMap();

			for (Token t : blob.getInputs()) {
				if (localBufferMap.containsKey(t)) {
					assert !inputChannels.containsKey(t) : "Same channels is exists in both localBuffer and inputChannel";
					bufferMapBuilder.put(t, localBufferMap.get(t));
				} else if (inputChannels.containsKey(t)) {
					BoundaryInputChannel chnl = inputChannels.get(t);
					bufferMapBuilder.put(t, chnl.getBuffer());
				} else {
					throw new AssertionError(String.format(
							"No Buffer for input channel %s ", t));
				}
			}

			for (Token t : blob.getOutputs()) {
				if (localBufferMap.containsKey(t)) {
					assert !outputChannels.containsKey(t) : "Same channels is exists in both localBuffer and outputChannel";
					bufferMapBuilder.put(t, localBufferMap.get(t));
				} else if (outputChannels.containsKey(t)) {
					BoundaryOutputChannel chnl = outputChannels.get(t);
					bufferMapBuilder.put(t, chnl.getBuffer());
				} else {
					throw new AssertionError(String.format(
							"No Buffer for output channel %s ", t));
				}
			}
			return bufferMapBuilder.build();
		}

		private void doDrain(boolean reqDrainData) {
			// System.out.println("Blob " + blobID + "is doDrain");
			this.reqDrainData = reqDrainData;
			drainState = 1;

			int stopType;
			// TODO: [2014-02-05] rearranged this order to call stop(3)
			// whenever GlobalConstants.useDrainData is false irrespective
			// of reqDrainData.
			if (GlobalConstants.useDrainData)
				if (!this.reqDrainData)
					stopType = 1;
				else
					stopType = 2;
			else
				stopType = 3;

			inChnlManager.stop(stopType);
			// TODO: [2014-03-14] I commented following lines to avoid one dead
			// lock case when draining. Deadlock 5 and 6.
			// inChnlManager.waitToStop();

			if (this.blob != null) {
				DrainCallback dcb = new DrainCallback(this);
				drainState = 2;
				this.blob.drain(dcb);
			}
			// System.out.println("Blob " + blobID +
			// "this.blob.drain(dcb); passed");

			if (useDrainDeadLockHandler) {
				boolean isLastBlob = true;
				for (BlobExecuter be : blobExecuters.values()) {
					if (be.drainState == 0) {
						isLastBlob = false;
						break;
					}
				}

				if (isLastBlob && drainDeadLockHandler == null) {
					System.out.println("****Starting DrainDeadLockHandler***");
					drainDeadLockHandler = new DrainDeadLockHandler();
					drainDeadLockHandler.start();
				}
			}
		}

		private void drained() {
			// System.out.println("Blob " + blobID + "drained at beg");
			if (drainState < 3)
				drainState = 3;
			else
				return;

			for (BlobThread2 bt : blobThreads) {
				bt.requestStop();
			}

			outChnlManager.stop(!this.reqDrainData);
			outChnlManager.waitToStop();

			if (drainState > 3)
				return;

			drainState = 4;
			SNMessageElement drained = new SNDrainElement.Drained(blobID);
			try {
				streamNode.controllerConnection.writeObject(drained);
			} catch (IOException e) {
				e.printStackTrace();
			}
			// System.out.println("Blob " + blobID + "is drained at mid");

			if (GlobalConstants.useDrainData && this.reqDrainData) {
				SNMessageElement me;
				if (crashed.get())
					me = getEmptyDrainData();
				else
					me = getDrainData();

				try {
					streamNode.controllerConnection.writeObject(me);
					// System.out.println(blobID + " DrainData has been sent");
					drainState = 6;

				} catch (IOException e) {
					e.printStackTrace();
				}
				// System.out.println("**********************************");
			}

			this.blob = null;
			boolean isLastBlob = true;
			for (BlobExecuter be : blobExecuters.values()) {
				if (be.drainState < 4) {
					isLastBlob = false;
					break;
				}
			}

			if (isLastBlob) {
				if (monBufs != null)
					monBufs.stopMonitoring();

				if (drainDeadLockHandler != null)
					drainDeadLockHandler.stopit();

			}
			// printDrainedStatus();
		}

		private DrainedData getDrainData() {
			if (this.blob == null)
				return getEmptyDrainData();
			// System.out.println("**********************************");
			DrainData dd = blob.getDrainData();
			drainState = 5;

			if (dd != null) {
				// for (Token t : dd.getData().keySet()) {
				// System.out.println("From Blob: " + t.toString() + " - "
				// + dd.getData().get(t).size());
				// }
			}

			ImmutableMap.Builder<Token, ImmutableList<Object>> inputDataBuilder = new ImmutableMap.Builder<>();
			ImmutableMap.Builder<Token, ImmutableList<Object>> outputDataBuilder = new ImmutableMap.Builder<>();

			ImmutableMap<Token, BoundaryInputChannel> inputChannels = inChnlManager
					.inputChannelsMap();
			for (Token t : blob.getInputs()) {
				if (inputChannels.containsKey(t)) {
					BoundaryChannel chanl = inputChannels.get(t);
					ImmutableList<Object> draindata = chanl
							.getUnprocessedData();
					// System.out.println(String.format(
					// "No of unprocessed data of %s is %d",
					// chanl.name(), draindata.size()));
					inputDataBuilder.put(t, draindata);
				}

				// TODO: Unnecessary data copy. Optimise this.
				else {
					Buffer buf = bufferMap.get(t);
					Object[] bufArray = new Object[buf.size()];
					buf.readAll(bufArray);
					assert buf.size() == 0 : String.format(
							"buffer size is %d. But 0 is expected", buf.size());
					inputDataBuilder.put(t, ImmutableList.copyOf(bufArray));
				}
			}

			ImmutableMap<Token, BoundaryOutputChannel> outputChannels = outChnlManager
					.outputChannelsMap();
			for (Token t : blob.getOutputs()) {
				if (outputChannels.containsKey(t)) {
					BoundaryChannel chanl = outputChannels.get(t);
					ImmutableList<Object> draindata = chanl
							.getUnprocessedData();
					// System.out.println(String.format(
					// "No of unprocessed data of %s is %d",
					// chanl.name(), draindata.size()));
					outputDataBuilder.put(t, draindata);
				}
			}

			return new SNDrainElement.DrainedData(blobID, dd,
					inputDataBuilder.build(), outputDataBuilder.build());
		}

		private DrainedData getEmptyDrainData() {
			drainState = 5;
			ImmutableMap.Builder<Token, ImmutableList<Object>> inputDataBuilder = new ImmutableMap.Builder<>();
			ImmutableMap.Builder<Token, ImmutableList<Object>> outputDataBuilder = new ImmutableMap.Builder<>();
			ImmutableMap.Builder<Token, ImmutableList<Object>> dataBuilder = ImmutableMap
					.builder();
			ImmutableTable.Builder<Integer, String, Object> stateBuilder = ImmutableTable
					.builder();
			DrainData dd = new DrainData(dataBuilder.build(),
					stateBuilder.build());
			return new SNDrainElement.DrainedData(blobID, dd,
					inputDataBuilder.build(), outputDataBuilder.build());
		}

		/**
		 * Returns a name for thread.
		 * 
		 * @param blob
		 * @return
		 */
		private String getName(Blob blob) {
			StringBuilder sb = new StringBuilder("Workers-");
			int limit = 0;
			for (Worker<?, ?> w : blob.getWorkers()) {
				sb.append(Workers.getIdentifier(w));
				sb.append(",");
				if (++limit > 5)
					break;
			}
			return sb.toString();
		}

		private void start() {
			outChnlManager.waitToStart();
			inChnlManager.waitToStart();

			bufferMap = buildBufferMap();
			blob.installBuffers(bufferMap);

			for (Thread t : blobThreads)
				t.start();

			System.out.println(blobID + " started");
		}

		private void startChannels() {
			outChnlManager.start();
			inChnlManager.start();
		}

		private void stop() {
			inChnlManager.stop(1);
			outChnlManager.stop(true);

			for (Thread t : blobThreads) {
				try {
					t.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			inChnlManager.waitToStop();
			outChnlManager.waitToStop();

			if (monBufs != null)
				monBufs.stopMonitoring();
		}
	}

	private final class BlobThread2 extends Thread {

		private final BlobExecuter be;

		private final Runnable coreCode;

		private volatile boolean stopping = false;

		private BlobThread2(Runnable coreCode, BlobExecuter be) {
			this.coreCode = coreCode;
			this.be = be;
		}

		private BlobThread2(Runnable coreCode, BlobExecuter be, String name) {
			super(name);
			this.coreCode = coreCode;
			this.be = be;
		}

		public void requestStop() {
			stopping = true;
		}

		@Override
		public void run() {
			try {
				while (!stopping)
					coreCode.run();
			} catch (Error | Exception e) {
				System.out.println(Thread.currentThread().getName()
						+ " crashed...");
				if (be.crashed.compareAndSet(false, true)) {
					e.printStackTrace();
					if (be.drainState == 1 || be.drainState == 2)
						be.drained();
					else if (be.drainState == 0) {
						try {
							streamNode.controllerConnection
									.writeObject(AppStatus.ERROR);
						} catch (IOException e1) {
							e1.printStackTrace();
						}
					}
				}
			}
		}
	}

	/**
	 * {@link CommandProcessor} at {@link StreamNode} side.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since May 27, 2013
	 */
	private class CommandProcessorImpl implements CommandProcessor {

		@Override
		public void processSTART() {
			start();
			long heapMaxSize = Runtime.getRuntime().maxMemory();
			long heapSize = Runtime.getRuntime().totalMemory();
			long heapFreeSize = Runtime.getRuntime().freeMemory();

			System.out
					.println("##############################################");

			System.out.println("heapMaxSize = " + heapMaxSize / 1e6);
			System.out.println("heapSize = " + heapSize / 1e6);
			System.out.println("heapFreeSize = " + heapFreeSize / 1e6);
			System.out.println("StraemJit app is running...");
			System.out
					.println("##############################################");

		}

		@Override
		public void processSTOP() {
			stop();
			System.out.println("StraemJit app stopped...");
			try {
				streamNode.controllerConnection.writeObject(AppStatus.STOPPED);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Implementation of {@link DrainProcessor} at {@link StreamNode} side. All
	 * appropriate response logic to successfully perform the draining is
	 * implemented here.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since Jul 30, 2013
	 */
	private class CTRLRDrainProcessorImpl implements CTRLRDrainProcessor {

		@Override
		public void process(DoDrain drain) {
			drain(drain.blobID, drain.reqDrainData);
		}

		@Override
		public void process(DrainDataRequest drnDataReq) {
			System.err.println("Not expected in current situation");
			// reqDrainedData(drnDataReq.blobsSet);
		}
	}

	private static class DrainCallback implements Runnable {

		private final BlobExecuter blobExec;

		// TODO: [2014-03-17] Just to added for checking the drain time. Remove
		// it later.
		private final Stopwatch sw;

		DrainCallback(BlobExecuter be) {
			this.blobExec = be;
			sw = Stopwatch.createStarted();
		}

		@Override
		public void run() {
			sw.stop();
			System.out.println("Time taken to drain " + blobExec.blobID
					+ " is " + sw.elapsed(TimeUnit.MILLISECONDS) + " ms");
			blobExec.drained();
		}
	}

	/**
	 * Handles another type of deadlock which occurs when draining. A Down blob,
	 * that has more than one upper blob, cannot progress because some of its
	 * upper blobs are drained and hence no input on the corresponding input
	 * channels, and other upper blobs blocked at their output channels as the
	 * down blob is no more consuming data. So those non-drained upper blobs are
	 * going to stuck forever at their output channels and the down blob will
	 * not receive DODrain command from the controller.
	 * 
	 * This class just discard the buffer contents so that blocked blobs can
	 * progress.
	 * 
	 * See the Deadlock 5.
	 * 
	 * @author sumanan
	 * 
	 */
	private class DrainDeadLockHandler extends Thread {

		final AtomicBoolean run;

		private DrainDeadLockHandler() {
			super("DrainDeadLockHandler");
			this.run = new AtomicBoolean(true);
		}

		public void run() {
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				return;
			}

			System.out
					.println("DrainDeadLockHandler is goint to clean buffers...");
			boolean areAllDrained = false;

			while (run.get()) {
				areAllDrained = cleanAllBuffers();
				if (areAllDrained)
					break;
			}
		}

		/**
		 * Go through all blocked blobs and clean all input and output buffers.
		 * This method is useful when we don't care about the drain data.
		 * 
		 * @return true iff there is no blocked blobs, i.e., all blobs have
		 *         completed the draining.
		 */
		private boolean cleanAllBuffers() {
			boolean areAllDrained = true;
			for (BlobExecuter be : blobExecuters.values()) {
				if (be.drainState == 1 || be.drainState == 2) {
					// System.out.println(be.blobID + " is not drained");
					areAllDrained = false;
					for (Token t : be.blob.getOutputs()) {
						Buffer b = be.bufferMap.get(t);
						clean(b, t);
					}

					for (Token t : be.blob.getInputs()) {
						Buffer b = be.bufferMap.get(t);
						clean(b, t);
					}
				}
			}
			return areAllDrained;
		}

		private void clean(Buffer b, Token t) {
			int size = b.size();
			if (size == 0)
				return;
			System.out.println(String.format(
					"Buffer %s has %d data. Going to clean it", t.toString(),
					size));
			Object[] obArray = new Object[size];
			b.readAll(obArray);
		}

		public void stopit() {
			this.run.set(false);
			this.interrupt();
		}
	}

	private static int count = 0;

	/**
	 * Profiles the buffer sizes in a timely manner and log that information
	 * into a text file. This information may be useful to analyse and find out
	 * deadlock situations.
	 * 
	 * @author sumanan
	 * 
	 */
	private class MonitorBuffers extends Thread {

		private final int id;

		private final AtomicBoolean stopFlag;

		int sleepTime = 25000;

		MonitorBuffers() {
			super("MonitorBuffers");
			stopFlag = new AtomicBoolean(false);
			id = count++;
		}

		public void run() {
			FileWriter writter = null;
			try {
				writter = new FileWriter(String.format("BufferStatus%d.txt",
						streamNode.getNodeID()), false);

				writter.write(String.format(
						"********Started*************** - %d\n", id));
				while (!stopFlag.get()) {
					try {
						Thread.sleep(sleepTime);
					} catch (InterruptedException e) {
					}
					if (blobExecuters == null) {
						writter.write("Buffer map is null...\n");
						continue;
					}
					writter.write("----------------------------------\n");
					for (BlobExecuter be : blobExecuters.values()) {
						if (be.bufferMap == null) {
							writter.write("Buffer map is null...\n");
							continue;
						}
						if (stopFlag.get())
							break;

						for (Map.Entry<Token, Buffer> en : be.bufferMap
								.entrySet()) {
							writter.write(en.getKey() + " - "
									+ en.getValue().size());
							writter.write('\n');
						}
					}
					writter.write("----------------------------------\n");
					writter.flush();
				}

				writter.write(String.format(
						"********Stopped*************** - %d\n", id));
			} catch (IOException e1) {
				e1.printStackTrace();
				return;
			}

			try {
				if (writter != null)
					writter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void stopMonitoring() {
			// System.out.println("MonitorBuffers: Stop monitoring");
			stopFlag.set(true);
			this.interrupt();
		}
	}
}
