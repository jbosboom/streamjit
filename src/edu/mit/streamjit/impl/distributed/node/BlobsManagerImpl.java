package edu.mit.streamjit.impl.distributed.node;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.CTRLRDrainProcessor;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.DoDrain;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.DrainDataRequest;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.DrainType;
import edu.mit.streamjit.impl.distributed.common.Command.CommandProcessor;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionProvider;
import edu.mit.streamjit.impl.distributed.common.SNTimeInfo;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.impl.distributed.node.BufferManager.SNLocalBufferManager;
import edu.mit.streamjit.impl.distributed.profiler.SNProfileElement;
import edu.mit.streamjit.impl.distributed.profiler.SNProfileElement.SNBufferStatusData;
import edu.mit.streamjit.impl.distributed.profiler.SNProfileElement.SNBufferStatusData.BlobBufferStatus;
import edu.mit.streamjit.impl.distributed.profiler.SNProfileElement.SNBufferStatusData.BufferStatus;
import edu.mit.streamjit.impl.distributed.profiler.StreamNodeProfiler;

/**
 * {@link BlobsManagerImpl} responsible to run all {@link Blob}s those are
 * assigned to the {@link StreamNode}.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
public class BlobsManagerImpl implements BlobsManager {

	Map<Token, BlobExecuter> blobExecuters;

	final BufferManager bufferManager;

	private final CommandProcessor cmdProcessor;

	private final Map<Token, ConnectionInfo> conInfoMap;

	private final ConnectionProvider conProvider;

	volatile BufferCleaner bufferCleaner = null;

	private final CTRLRDrainProcessor drainProcessor;

	MonitorBuffers monBufs = null;

	final StreamNode streamNode;

	/**
	 * if true {@link BufferCleaner} will be used to unlock the draining time
	 * dead lock. Otherwise dynamic buffer will be used for local buffers to
	 * handled drain time data growth.
	 */
	final boolean useBufferCleaner = false;

	/**
	 * if true {@link MonitorBuffers} will be started to log the buffer sizes
	 * periodically.
	 */
	private final boolean monitorBuffers = false;

	private final String appName;

	private ImmutableSet<StreamNodeProfiler> profilers;

	public BlobsManagerImpl(ImmutableSet<Blob> blobSet,
			Map<Token, ConnectionInfo> conInfoMap, StreamNode streamNode,
			ConnectionProvider conProvider, String appName) {
		this.conInfoMap = conInfoMap;
		this.streamNode = streamNode;
		this.conProvider = conProvider;

		this.cmdProcessor = new CommandProcessorImpl();
		this.drainProcessor = new CTRLRDrainProcessorImpl();
		this.bufferManager = new SNLocalBufferManager(blobSet);

		this.appName = appName;
		bufferManager.initialise();
		if (bufferManager.isbufferSizesReady())
			createBEs(blobSet);
	}

	/**
	 * Drain the blob identified by the token.
	 */
	public void drain(Token blobID, DrainType drainType) {
		for (BlobExecuter be : blobExecuters.values()) {
			if (be.getBlobID().equals(blobID)) {
				be.doDrain(drainType);
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

		if (monitorBuffers && monBufs == null) {
			// System.out.println("Creating new MonitorBuffers");
			monBufs = new MonitorBuffers();
			monBufs.start();
		}
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

		if (bufferCleaner != null)
			bufferCleaner.stopit();
	}

	@Override
	public Set<StreamNodeProfiler> profilers() {
		if (profilers == null) {
			StreamNodeProfiler snp = new BufferProfiler();
			profilers = ImmutableSet.of(snp);
		}
		return profilers;
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
			blobExecuters.put(t, new BlobExecuter(this, t, b, inputChannels,
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
	synchronized void printDrainedStatus() {
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

	final class BlobThread2 extends Thread {

		private final BlobExecuter be;

		private final Runnable coreCode;

		private volatile boolean stopping = false;

		BlobThread2(Runnable coreCode, BlobExecuter be) {
			this.coreCode = coreCode;
			this.be = be;
		}

		BlobThread2(Runnable coreCode, BlobExecuter be, String name) {
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
			System.out.println("StraemJit app is running...");
			Utils.printMemoryStatus();
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
			drain(drain.blobID, drain.drainType);
		}

		@Override
		public void process(DrainDataRequest drnDataReq) {
			System.err.println("Not expected in current situation");
			// reqDrainedData(drnDataReq.blobsSet);
		}
	}

	class DrainCallback implements Runnable {

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
			long time = sw.elapsed(TimeUnit.MILLISECONDS);
			// System.out.println("Time taken to drain " + blobExec.blobID +
			// " is " + time + " ms");
			try {
				streamNode.controllerConnection
						.writeObject(new SNTimeInfo.DrainingTime(
								blobExec.blobID, time));
			} catch (IOException e) {
				e.printStackTrace();
			}
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
	class BufferCleaner extends Thread {

		final AtomicBoolean run;

		final boolean needToCopyDrainData;

		final Map<Token, List<Object[]>> newlocalBufferMap;

		BufferCleaner(boolean needToCopyDrainData) {
			super("BufferCleaner");
			System.out.println("Buffer Cleaner : needToCopyDrainData == "
					+ needToCopyDrainData);
			this.run = new AtomicBoolean(true);
			this.needToCopyDrainData = needToCopyDrainData;
			if (needToCopyDrainData)
				newlocalBufferMap = new HashMap<>();
			else
				newlocalBufferMap = null;
		}

		public void run() {
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				return;
			}

			System.out.println("BufferCleaner is going to clean buffers...");
			boolean areAllDrained = false;

			while (run.get()) {
				if (needToCopyDrainData)
					areAllDrained = copyLocalBuffers();
				else
					areAllDrained = cleanAllBuffers();

				if (areAllDrained)
					break;

				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					break;
				}
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

		/**
		 * Copy only the local buffers into a new large buffer to make the
		 * blocked blob to progress. This copied buffer can be sent to
		 * controller as a drain data.
		 */
		private boolean copyLocalBuffers() {
			ImmutableMap<Token, LocalBuffer> localBufferMap = bufferManager
					.localBufferMap();
			boolean areAllDrained = true;
			for (BlobExecuter be : blobExecuters.values()) {
				if (be.drainState == 1 || be.drainState == 2) {
					// System.out.println(be.blobID + " is not drained");
					areAllDrained = false;
					for (Token t : be.blob.getOutputs()) {
						if (localBufferMap.containsKey(t)) {
							Buffer b = be.bufferMap.get(t);
							copy(b, t);
						}
					}
				}
			}
			return areAllDrained;
		}

		private void copy(Buffer b, Token t) {
			int size = b.size();
			if (size == 0)
				return;

			if (!newlocalBufferMap.containsKey(t)) {
				newlocalBufferMap.put(t, new LinkedList<Object[]>());
			}

			List<Object[]> list = newlocalBufferMap.get(t);
			Object[] bufArray = new Object[size];
			b.readAll(bufArray);
			assert b.size() == 0 : String.format(
					"buffer size is %d. But 0 is expected", b.size());
			list.add(bufArray);
		}

		public void stopit() {
			this.run.set(false);
			this.interrupt();
		}

		public Object[] copiedBuffer(Token t) {
			assert needToCopyDrainData : "BufferCleaner is not in buffer copy mode";
			copy(bufferManager.localBufferMap().get(t), t);
			List<Object[]> list = newlocalBufferMap.get(t);
			if (list == null)
				return new Object[0];
			else if (list.size() == 0)
				return new Object[0];
			else if (list.size() == 1)
				return list.get(0);

			int size = 0;
			for (Object[] array : list) {
				size += array.length;
			}

			int destPos = 0;
			Object[] mergedArray = new Object[size];
			for (Object[] array : list) {
				System.arraycopy(array, 0, mergedArray, destPos, array.length);
				destPos += array.length;
			}
			return mergedArray;
		}
	}

	private class BlobsBufferStatus {

		/**
		 * @return Status of all buffers of all blobs of this
		 *         {@link BlobsManager}.
		 */
		private SNBufferStatusData snBufferStatusData() {
			Set<BlobBufferStatus> blobBufferStatusSet = new HashSet<>();
			if (blobExecuters != null) {
				for (BlobExecuter be : blobExecuters.values()) {
					blobBufferStatusSet.add(blobBufferStatus(be));
				}
			}

			return new SNBufferStatusData(streamNode.getNodeID(),
					ImmutableSet.copyOf(blobBufferStatusSet));
		}

		/**
		 * Status of the all buffers of the blob represented by the @param be.
		 * 
		 * @param be
		 * @return
		 */
		private BlobBufferStatus blobBufferStatus(BlobExecuter be) {
			return new BlobBufferStatus(be.blobID, bufferStatusSet(be, true),
					bufferStatusSet(be, false));
		}

		/**
		 * @param be
		 * @param isIn
		 *            Decides whether a blob's inputbuffer's status or
		 *            outputbuffers's status should be returned.
		 * @return Set of {@link BufferStatus} of a blob's set of input buffers
		 *         or set of output buffers depends on isIn argument.
		 */
		private ImmutableSet<BufferStatus> bufferStatusSet(BlobExecuter be,
				boolean isIn) {
			if (be.bufferMap == null)
				return ImmutableSet.of();

			Set<Token> tokenSet = tokenSet(be, isIn);
			Set<BufferStatus> bufferStatus = new HashSet<>();
			for (Token t : tokenSet) {
				bufferStatus.add(bufferStatus(t, be, isIn));
			}
			return ImmutableSet.copyOf(bufferStatus);
		}

		private BufferStatus bufferStatus(Token bufferID, BlobExecuter be,
				boolean isIn) {
			int min = Integer.MAX_VALUE;
			// BE sets blob to null after the drained().
			if (be.blob != null)
				min = be.blob.getMinimumBufferCapacity(bufferID);

			int availableResource = min;
			Buffer b = be.bufferMap.get(bufferID);
			if (b != null)
				availableResource = isIn ? b.size() : b.capacity() - b.size();

			return new BufferStatus(bufferID, min, availableResource);
		}

		/**
		 * Return a blob's either input or output buffer's token set.
		 * 
		 * @param be
		 * @param isIn
		 *            Decides whether a blob's inputbuffer's token set or
		 *            outputbuffers's token set should be returned.
		 * @return Blob's inputbuffer's token set or outputbuffers's token set.
		 */
		private Set<Token> tokenSet(BlobExecuter be, boolean isIn) {
			Set<Token> tokenSet;
			// BE sets blob to null after the drained().
			if (be.blob == null) {
				if (isIn)
					tokenSet = be.inChnlManager.inputChannelsMap().keySet();
				else
					tokenSet = be.outChnlManager.outputChannelsMap().keySet();
			} else {
				if (isIn)
					tokenSet = be.blob.getInputs();
				else
					tokenSet = be.blob.getOutputs();
			}
			return tokenSet;
		}
	}

	private static int count = 0;

	/**
	 * TODO: [27-01-2015] Use BufferProfiler to get buffer status and then write
	 * the status in to the file. I created BufferProfiler by copying most of
	 * the code from this class.
	 * <p>
	 * Profiles the buffer sizes in a timely manner and log that information
	 * into a text file. This information may be useful to analyse and find out
	 * deadlock situations.
	 * 
	 * @author sumanan
	 * 
	 */
	class MonitorBuffers extends Thread {

		private final int id;

		private final AtomicBoolean stopFlag;

		int sleepTime = 25000;

		MonitorBuffers() {
			super("MonitorBuffers");
			stopFlag = new AtomicBoolean(false);
			id = count++;
		}

		public void run() {
			FileWriter writer = null;
			try {
				String fileName = String.format("%s%sBufferStatus%d.txt",
						appName, File.separator, streamNode.getNodeID());
				writer = new FileWriter(fileName, false);

				writer.write(String.format(
						"********Started*************** - %d\n", id));
				while (!stopFlag.get()) {
					try {
						Thread.sleep(sleepTime);
					} catch (InterruptedException e) {
						break;
					}

					if (stopFlag.get())
						break;

					if (blobExecuters == null) {
						writer.write("blobExecuters are null...\n");
						continue;
					}

					writer.write("----------------------------------\n");
					for (BlobExecuter be : blobExecuters.values()) {
						writer.write("Status of blob " + be.blobID.toString()
								+ "\n");

						if (be.bufferMap == null) {
							writer.write("Buffer map is null...\n");
							continue;
						}

						if (stopFlag.get())
							break;

						writer.write("Input channel details\n");
						write(be, writer, true);

						writer.write("Output channel details\n");
						write(be, writer, false);
					}
					writer.write("----------------------------------\n");
					writer.flush();
				}

				writer.write(String.format(
						"********Stopped*************** - %d\n", id));
			} catch (IOException e1) {
				e1.printStackTrace();
				return;
			}

			try {
				if (writer != null)
					writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void write(BlobExecuter be, FileWriter writer, boolean isIn)
				throws IOException {
			Set<Token> tokenSet = tokenSet(be, isIn);
			for (Token t : tokenSet) {
				Buffer b = be.bufferMap.get(t);
				if (b == null)
					continue;
				int min = Integer.MAX_VALUE;
				// BE sets blob to null after the drained().
				if (be.blob != null)
					min = be.blob.getMinimumBufferCapacity(t);

				int availableResource = isIn ? b.size() : b.capacity()
						- b.size();

				String status = availableResource >= min ? "Firable"
						: "NOT firable";
				writer.write(t.toString() + "\tMin - " + min
						+ ",\tAvailableResource - " + availableResource + "\t"
						+ status + "\n");
			}
		}

		private Set<Token> tokenSet(BlobExecuter be, boolean isIn) {
			Set<Token> tokenSet;
			// BE sets blob to null after the drained().
			if (be.blob == null) {
				if (isIn)
					tokenSet = be.inChnlManager.inputChannelsMap().keySet();
				else
					tokenSet = be.outChnlManager.outputChannelsMap().keySet();
			} else {
				if (isIn)
					tokenSet = be.blob.getInputs();
				else
					tokenSet = be.blob.getOutputs();
			}
			return tokenSet;
		}

		public void stopMonitoring() {
			// System.out.println("MonitorBuffers: Stop monitoring");
			stopFlag.set(true);
			this.interrupt();
		}
	}

	public class BufferProfiler implements StreamNodeProfiler {

		BlobsBufferStatus bbs = new BlobsBufferStatus();

		@Override
		public SNProfileElement profile() {
			return bbs.snBufferStatusData();
		}
	}
}
