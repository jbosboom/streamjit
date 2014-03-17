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
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.ConcurrentArrayBuffer;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.CTRLRDrainProcessor;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.DoDrain;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.DrainDataRequest;
import edu.mit.streamjit.impl.distributed.common.Command.CommandProcessor;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement;
import edu.mit.streamjit.impl.distributed.common.SNMessageElement;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.DrainedData;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionProvider;
import edu.mit.streamjit.impl.distributed.common.Utils;

/**
 * {@link BlobsManagerImpl} responsible to run all {@link Blob}s those are
 * assigned to the {@link StreamNode}.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
public class BlobsManagerImpl implements BlobsManager {

	private Set<BlobExecuter> blobExecuters;
	private final StreamNode streamNode;
	private final TCPConnectionProvider conProvider;
	private Map<Token, TCPConnectionInfo> conInfoMap;

	private MonitorBuffers monBufs;

	private final CTRLRDrainProcessor drainProcessor;

	private final CommandProcessor cmdProcessor;

	private final ImmutableMap<Token, Buffer> bufferMap;

	public BlobsManagerImpl(ImmutableSet<Blob> blobSet,
			Map<Token, TCPConnectionInfo> conInfoMap, StreamNode streamNode,
			TCPConnectionProvider conProvider) {
		this.conInfoMap = conInfoMap;
		this.streamNode = streamNode;
		this.conProvider = conProvider;

		this.cmdProcessor = new CommandProcessorImpl();
		this.drainProcessor = new CTRLRDrainProcessorImpl();

		bufferMap = createBufferMap(blobSet);

		for (Blob b : blobSet) {
			b.installBuffers(bufferMap);
		}

		Set<Token> locaTokens = getLocalTokens(blobSet);
		blobExecuters = new HashSet<>();
		for (Blob b : blobSet) {
			ImmutableMap<Token, BoundaryInputChannel> inputChannels = createInputChannels(
					Sets.difference(b.getInputs(), locaTokens), bufferMap);
			ImmutableMap<Token, BoundaryOutputChannel> outputChannels = createOutputChannels(
					Sets.difference(b.getOutputs(), locaTokens), bufferMap);
			blobExecuters
					.add(new BlobExecuter(b, inputChannels, outputChannels));
		}
	}

	/**
	 * Start and execute the blobs. This function should be responsible to
	 * manage all CPU and I/O threads those are related to the {@link Blob}s.
	 */
	public void start() {
		for (BlobExecuter be : blobExecuters)
			be.start();

		if (monBufs == null) {
			// System.out.println("Creating new MonitorBuffers");
			monBufs = new MonitorBuffers();
			monBufs.start();
		} else
			System.err
					.println("Mon buffer is not null. Check the logic for bug");
	}

	/**
	 * Stop all {@link Blob}s if running. No effect if a {@link Blob} is already
	 * stopped.
	 */
	public void stop() {
		for (BlobExecuter be : blobExecuters)
			be.stop();

		if (monBufs != null)
			monBufs.stopMonitoring();
	}

	// TODO: Buffer sizes, including head and tail buffers, must be optimized.
	// consider adding some tuning factor
	private ImmutableMap<Token, Buffer> createBufferMap(Set<Blob> blobSet) {
		ImmutableMap.Builder<Token, Buffer> bufferMapBuilder = ImmutableMap
				.<Token, Buffer> builder();

		Map<Token, Integer> minInputBufCapaciy = new HashMap<>();
		Map<Token, Integer> minOutputBufCapaciy = new HashMap<>();

		for (Blob b : blobSet) {
			Set<Blob.Token> inputs = b.getInputs();
			for (Token t : inputs) {
				minInputBufCapaciy.put(t, b.getMinimumBufferCapacity(t));
			}

			Set<Blob.Token> outputs = b.getOutputs();
			for (Token t : outputs) {
				minOutputBufCapaciy.put(t, b.getMinimumBufferCapacity(t));
			}
		}

		Set<Token> localTokens = Sets.intersection(minInputBufCapaciy.keySet(),
				minOutputBufCapaciy.keySet());
		Set<Token> globalInputTokens = Sets.difference(
				minInputBufCapaciy.keySet(), localTokens);
		Set<Token> globalOutputTokens = Sets.difference(
				minOutputBufCapaciy.keySet(), localTokens);

		for (Token t : localTokens) {
			int bufSize = Math.max(minInputBufCapaciy.get(t),
					minOutputBufCapaciy.get(t));
			addBuffer(t, bufSize, bufferMapBuilder);
		}

		for (Token t : globalInputTokens) {
			int bufSize = minInputBufCapaciy.get(t);
			addBuffer(t, bufSize, bufferMapBuilder);
		}

		for (Token t : globalOutputTokens) {
			int bufSize = minOutputBufCapaciy.get(t);
			addBuffer(t, bufSize, bufferMapBuilder);
		}
		return bufferMapBuilder.build();
	}

	/**
	 * Just introduced to avoid code duplication.
	 * 
	 * @param t
	 * @param minSize
	 * @param bufferMapBuilder
	 */
	private void addBuffer(Token t, int minSize,
			ImmutableMap.Builder<Token, Buffer> bufferMapBuilder) {
		// TODO: Just to increase the performance. Change it later
		int bufSize = Math.max(1000, minSize);
		// System.out.println("Buffer size of " + t.toString() + " is " +
		// bufSize);
		bufferMapBuilder.put(t, new ConcurrentArrayBuffer(bufSize));
	}

	private long gcd(long a, long b) {
		while (true) {
			if (a == 0)
				return b;
			b %= a;
			if (b == 0)
				return a;
			a %= b;
		}
	}

	private long lcm(long a, long b) {
		long val = gcd(a, b);
		long quotient = a / val;
		return val != 0 ? b * quotient : 0;
	}

	private Set<Token> getLocalTokens(Set<Blob> blobSet) {
		Set<Token> inputTokens = new HashSet<>();
		Set<Token> outputTokens = new HashSet<>();

		for (Blob b : blobSet) {
			Set<Token> inputs = b.getInputs();
			for (Token t : inputs) {
				inputTokens.add(t);
			}

			Set<Token> outputs = b.getOutputs();
			for (Token t : outputs) {
				outputTokens.add(t);
			}
		}
		return Sets.intersection(inputTokens, outputTokens);
	}

	private ImmutableMap<Token, BoundaryInputChannel> createInputChannels(
			Set<Token> inputTokens, ImmutableMap<Token, Buffer> bufferMap) {
		ImmutableMap.Builder<Token, BoundaryInputChannel> inputChannelMap = new ImmutableMap.Builder<>();
		for (Token t : inputTokens) {
			TCPConnectionInfo conInfo = conInfoMap.get(t);
			inputChannelMap.put(t, new TCPInputChannel(bufferMap.get(t),
					conProvider, conInfo, t.toString(), 0));
		}
		return inputChannelMap.build();
	}

	private ImmutableMap<Token, BoundaryOutputChannel> createOutputChannels(
			Set<Token> outputTokens, ImmutableMap<Token, Buffer> bufferMap) {
		ImmutableMap.Builder<Token, BoundaryOutputChannel> outputChannelMap = new ImmutableMap.Builder<>();
		for (Token t : outputTokens) {
			TCPConnectionInfo conInfo = conInfoMap.get(t);
			outputChannelMap.put(t, new TCPOutputChannel(bufferMap.get(t),
					conProvider, conInfo, t.toString(), 0));
		}
		return outputChannelMap.build();
	}

	private class BlobExecuter {

		private AtomicBoolean crashed;
		private volatile int drainState;
		private final Token blobID;

		private final Blob blob;
		private Set<BlobThread2> blobThreads;

		private final ImmutableMap<Token, BoundaryInputChannel> inputChannels;
		private final ImmutableMap<Token, BoundaryOutputChannel> outputChannels;

		Set<Thread> inputChannelThreads;
		Set<Thread> outputChannelThreads;

		private boolean reqDrainData;

		private BlobExecuter(Blob blob,
				ImmutableMap<Token, BoundaryInputChannel> inputChannels,
				ImmutableMap<Token, BoundaryOutputChannel> outputChannels) {
			this.crashed = new AtomicBoolean(false);
			this.blob = blob;
			this.blobThreads = new HashSet<>();
			assert blob.getInputs().containsAll(inputChannels.keySet());
			assert blob.getOutputs().containsAll(outputChannels.keySet());
			this.inputChannels = inputChannels;
			this.outputChannels = outputChannels;
			inputChannelThreads = new HashSet<>(inputChannels.values().size());
			outputChannelThreads = new HashSet<>(outputChannels.values().size());

			for (int i = 0; i < blob.getCoreCount(); i++) {
				StringBuilder sb = new StringBuilder("Workers-");
				for (Worker<?, ?> w : blob.getWorkers()) {
					sb.append(Workers.getIdentifier(w));
					sb.append(",");
				}
				blobThreads.add(new BlobThread2(blob.getCoreCode(i), this, sb
						.toString()));
			}

			drainState = 0;
			this.blobID = Utils.getBlobID(blob);
		}

		private void start() {
			for (BoundaryInputChannel bc : inputChannels.values()) {
				Thread t = new Thread(bc.getRunnable(), bc.name());
				t.start();
				inputChannelThreads.add(t);
			}

			for (BoundaryOutputChannel bc : outputChannels.values()) {
				Thread t = new Thread(bc.getRunnable(), bc.name());
				t.start();
				outputChannelThreads.add(t);
			}

			for (Thread t : blobThreads)
				t.start();

			System.out.println(blobID + " started");
		}

		private void stop() {

			for (BoundaryInputChannel bc : inputChannels.values()) {
				bc.stop(1);
			}

			for (BoundaryOutputChannel bc : outputChannels.values()) {
				bc.stop(true);
			}

			for (Thread t : blobThreads)
				try {
					t.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			if (monBufs != null)
				monBufs.stopMonitoring();
		}

		private void doDrain(boolean reqDrainData) {
			// System.out.println("Blob " + blobID + "is doDrain");
			this.reqDrainData = reqDrainData;
			drainState = 1;

			for (BoundaryInputChannel bc : inputChannels.values()) {
				// TODO: [2014-02-05] rearranged this order to call stop(3)
				// whenever GlobalConstants.useDrainData is false irrespective
				// of reqDrainData.
				if (GlobalConstants.useDrainData)
					if (!this.reqDrainData)
						bc.stop(1);
					else
						bc.stop(2);
				else
					bc.stop(3);
			}

			for (Thread t : inputChannelThreads) {
				try {
					t.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			// System.out.println("Blob " + blobID + "Input thread's joined");
			DrainCallback dcb = new DrainCallback(this);
			drainState = 2;
			this.blob.drain(dcb);
			// System.out.println("Blob " + blobID +
			// "this.blob.drain(dcb); passed");
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

			for (BoundaryOutputChannel bc : outputChannels.values()) {
				bc.stop(!this.reqDrainData);
			}

			for (Thread t : outputChannelThreads) {
				try {
					t.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

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

			boolean isLastBlob = true;
			for (BlobExecuter be : blobExecuters) {
				if (be.drainState < 4) {
					isLastBlob = false;
					break;
				}
			}

			if (isLastBlob && monBufs != null)
				monBufs.stopMonitoring();

			// printDrainedStatus();
		}

		private DrainedData getDrainData() {
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

		public Token getBlobID() {
			return Utils.getBlobID(blob);
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
	 * Drain the blob identified by the token.
	 */
	public void drain(Token blobID, boolean reqDrainData) {
		for (BlobExecuter be : blobExecuters) {
			if (be.getBlobID().equals(blobID)) {
				be.doDrain(reqDrainData);
				return;
			}
		}
		throw new IllegalArgumentException(String.format(
				"No blob with blobID %s", blobID));
	}

	/**
	 * Just to added for debugging purpose.
	 */
	private synchronized void printDrainedStatus() {
		System.out.println("****************************************");
		for (BlobExecuter be : blobExecuters) {
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

	public void reqDrainedData(Set<Token> blobSet) {
		// ImmutableMap.Builder<Token, DrainData> builder = new
		// ImmutableMap.Builder<>();
		// for (BlobExecuter be : blobExecuters) {
		// if (be.isDrained) {
		// builder.put(be.blobID, be.blob.getDrainData());
		// }
		// }
		//
		// try {
		// streamNode.controllerConnection
		// .writeObject(new SNDrainElement.DrainedData(builder.build()));
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
	}

	public CTRLRDrainProcessor getDrainProcessor() {
		return drainProcessor;
	}

	public CommandProcessor getCommandProcessor() {
		return cmdProcessor;
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
		public void process(DrainDataRequest drnDataReq) {
			System.err.println("Not expected in current situation");
			// reqDrainedData(drnDataReq.blobsSet);
		}

		@Override
		public void process(DoDrain drain) {
			drain(drain.blobID, drain.reqDrainData);
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

	private final class BlobThread2 extends Thread {
		private volatile boolean stopping = false;
		private final Runnable coreCode;
		private final BlobExecuter be;

		private BlobThread2(Runnable coreCode, BlobExecuter be, String name) {
			super(name);
			this.coreCode = coreCode;
			this.be = be;
		}

		private BlobThread2(Runnable coreCode, BlobExecuter be) {
			this.coreCode = coreCode;
			this.be = be;
		}

		@Override
		public void run() {
			try {
				while (!stopping)
					coreCode.run();
			} catch (Error | Exception e) {
				System.out.println(Thread.currentThread().getName()
						+ " crached...");
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
		public void requestStop() {
			stopping = true;
		}
	}

	private static int count = 0;

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
					if (bufferMap == null) {
						writter.write("Buffer map is null...\n");
						continue;
					}
					if (stopFlag.get())
						break;
					writter.write("----------------------------------\n");
					for (Map.Entry<Token, Buffer> en : bufferMap.entrySet()) {
						writter.write(en.getKey() + " - "
								+ en.getValue().size());
						writter.write('\n');
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
