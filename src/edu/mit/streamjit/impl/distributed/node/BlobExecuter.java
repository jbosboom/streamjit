package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;

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
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.DrainType;
import edu.mit.streamjit.impl.distributed.common.Connection;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.SNDrainedData;
import edu.mit.streamjit.impl.distributed.common.SNMessageElement;
import edu.mit.streamjit.impl.distributed.common.SNTimeInfo;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;

/**
 * This class was an inner class of {@link BlobsManagerImpl}. I have re factored
 * {@link BlobsManagerImpl} and moved this class a new file.
 * 
 * @author sumanan
 * @since 4 Feb, 2015
 */
class BlobExecuter {

	/**
	 * 
	 */
	private final BlobsManagerImpl blobsManagerImpl;

	Blob blob;

	final Token blobID;

	final private Set<BlobThread2> blobThreads;

	/**
	 * Buffers for all input and output edges of the {@link #blob}.
	 */
	ImmutableMap<Token, Buffer> bufferMap;

	private ImmutableMap<Token, LocalBuffer> outputLocalBuffers;

	/**
	 * This flag will be set to true if an exception thrown by the core code of
	 * the {@link Blob}. Any exception occurred in a blob's corecode will be
	 * informed to {@link Controller} to halt the application. See the
	 * {@link BlobThread2}.
	 */
	AtomicBoolean crashed;

	volatile int drainState;

	final BoundaryInputChannelManager inChnlManager;

	final BoundaryOutputChannelManager outChnlManager;

	private DrainType drainType;

	BlobExecuter(BlobsManagerImpl blobsManagerImpl, Token t, Blob blob,
			ImmutableMap<Token, BoundaryInputChannel> inputChannels,
			ImmutableMap<Token, BoundaryOutputChannel> outputChannels) {
		this.blobsManagerImpl = blobsManagerImpl;
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
			blobThreads.add(new BlobThread2(blob.getCoreCode(i), this, name));
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
	 * bufferMap will contain all input and output edges of the {@link #blob}.
	 * 
	 * Note that, Some {@link BoundaryChannel}s (e.g.,
	 * {@link AsyncOutputChannel}) create {@link Buffer}s after establishing
	 * {@link Connection} with other end. So this method must be called after
	 * establishing all IO connections.
	 * {@link InputChannelManager#waitToStart()} and
	 * {@link OutputChannelManager#waitToStart()} ensure that the IO connections
	 * are successfully established.
	 * 
	 * @return Buffer map which contains {@link Buffers} for all input and
	 *         output edges of the {@link #blob}.
	 */
	private ImmutableMap<Token, Buffer> buildBufferMap() {
		ImmutableMap.Builder<Token, Buffer> bufferMapBuilder = ImmutableMap
				.builder();
		ImmutableMap.Builder<Token, LocalBuffer> outputLocalBufferBuilder = ImmutableMap
				.builder();
		ImmutableMap<Token, LocalBuffer> localBufferMap = this.blobsManagerImpl.bufferManager
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
				LocalBuffer buf = localBufferMap.get(t);
				bufferMapBuilder.put(t, buf);
				outputLocalBufferBuilder.put(t, buf);
			} else if (outputChannels.containsKey(t)) {
				BoundaryOutputChannel chnl = outputChannels.get(t);
				bufferMapBuilder.put(t, chnl.getBuffer());
			} else {
				throw new AssertionError(String.format(
						"No Buffer for output channel %s ", t));
			}
		}
		outputLocalBuffers = outputLocalBufferBuilder.build();
		return bufferMapBuilder.build();
	}

	void doDrain(DrainType drainType) {
		// System.out.println("Blob " + blobID + "is doDrain");
		this.drainType = drainType;
		drainState = 1;

		inChnlManager.stop(drainType);
		// TODO: [2014-03-14] I commented following line to avoid one dead
		// lock case when draining. Deadlock 5 and 6.
		// [2014-09-17] Lets waitToStop() if drain data is required.
		if (drainType != DrainType.DISCARD)
			inChnlManager.waitToStop();

		for (LocalBuffer buf : outputLocalBuffers.values()) {
			buf.drainingStarted(drainType);
		}

		if (this.blob != null) {
			DrainCallback dcb = new DrainCallback(this);
			drainState = 2;
			this.blob.drain(dcb);
		}
		// System.out.println("Blob " + blobID +
		// "this.blob.drain(dcb); passed");

		if (this.blobsManagerImpl.useBufferCleaner
				&& drainType != DrainType.FINAL) {
			boolean isLastBlob = true;
			for (BlobExecuter be : this.blobsManagerImpl.blobExecuters.values()) {
				if (be.drainState == 0) {
					isLastBlob = false;
					break;
				}
			}

			if (isLastBlob && this.blobsManagerImpl.bufferCleaner == null) {
				System.out.println("****Starting BufferCleaner***");
				this.blobsManagerImpl.bufferCleaner = this.blobsManagerImpl.new BufferCleaner(
						drainType == DrainType.INTERMEDIATE);
				this.blobsManagerImpl.bufferCleaner.start();
			}
		}
	}

	void drained() {
		// System.out.println("Blob " + blobID + "drained at beg");
		if (drainState < 3)
			drainState = 3;
		else
			return;

		for (BlobThread2 bt : blobThreads) {
			bt.requestStop();
		}

		outChnlManager.stop(drainType == DrainType.FINAL);
		outChnlManager.waitToStop();

		if (drainState > 3)
			return;

		drainState = 4;
		SNMessageElement drained = new SNDrainElement.Drained(blobID);
		try {
			this.blobsManagerImpl.streamNode.controllerConnection
					.writeObject(drained);
		} catch (IOException e) {
			e.printStackTrace();
		}
		// System.out.println("Blob " + blobID + "is drained at mid");

		if (drainType != DrainType.DISCARD) {
			SNMessageElement me;
			if (crashed.get())
				me = getEmptyDrainData();
			else
				me = getSNDrainData();

			try {
				this.blobsManagerImpl.streamNode.controllerConnection
						.writeObject(me);
				// System.out.println(blobID + " DrainData has been sent");
				drainState = 6;

			} catch (IOException e) {
				e.printStackTrace();
			}
			// System.out.println("**********************************");
		}

		this.blob = null;
		boolean isLastBlob = true;
		for (BlobExecuter be : this.blobsManagerImpl.blobExecuters.values()) {
			if (be.drainState < 4) {
				isLastBlob = false;
				break;
			}
		}

		if (isLastBlob) {
			if (this.blobsManagerImpl.monBufs != null)
				this.blobsManagerImpl.monBufs.stopMonitoring();

			if (this.blobsManagerImpl.bufferCleaner != null)
				this.blobsManagerImpl.bufferCleaner.stopit();

		}
		// printDrainedStatus();
	}

	private SNDrainedData getSNDrainData() {
		if (this.blob == null)
			return getEmptyDrainData();

		DrainData dd = blob.getDrainData();
		drainState = 5;
		// printDrainDataStats(dd);

		ImmutableMap.Builder<Token, ImmutableList<Object>> inputDataBuilder = new ImmutableMap.Builder<>();
		ImmutableMap.Builder<Token, ImmutableList<Object>> outputDataBuilder = new ImmutableMap.Builder<>();

		ImmutableMap<Token, BoundaryInputChannel> inputChannels = inChnlManager
				.inputChannelsMap();

		// In a proper system the following line should be called inside
		// doDrain(), just after inChnlManager.stop(). Read the comment
		// in doDrain().
		inChnlManager.waitToStop();

		for (Token t : blob.getInputs()) {
			if (inputChannels.containsKey(t)) {
				BoundaryChannel chanl = inputChannels.get(t);
				ImmutableList<Object> draindata = chanl.getUnprocessedData();
				// if (draindata.size() > 0)
				// System.out.println(String.format("From %s - %d",
				// chanl.name(), draindata.size()));
				inputDataBuilder.put(t, draindata);
			}

			else {
				unprocessedDataFromLocalBuffer(inputDataBuilder, t);
			}
		}

		ImmutableMap<Token, BoundaryOutputChannel> outputChannels = outChnlManager
				.outputChannelsMap();
		for (Token t : blob.getOutputs()) {
			if (outputChannels.containsKey(t)) {
				BoundaryChannel chanl = outputChannels.get(t);
				ImmutableList<Object> draindata = chanl.getUnprocessedData();
				// if (draindata.size() > 0)
				// System.out.println(String.format("From %s - %d",
				// chanl.name(), draindata.size()));
				outputDataBuilder.put(t, draindata);
			}
		}

		return new SNDrainElement.SNDrainedData(blobID, dd,
				inputDataBuilder.build(), outputDataBuilder.build());
	}

	// TODO: Unnecessary data copy. Optimise this.
	private void unprocessedDataFromLocalBuffer(
			ImmutableMap.Builder<Token, ImmutableList<Object>> inputDataBuilder,
			Token t) {
		Object[] bufArray;
		if (this.blobsManagerImpl.bufferCleaner == null) {
			Buffer buf = bufferMap.get(t);
			bufArray = new Object[buf.size()];
			buf.readAll(bufArray);
			assert buf.size() == 0 : String.format(
					"buffer size is %d. But 0 is expected", buf.size());
		} else {
			bufArray = this.blobsManagerImpl.bufferCleaner.copiedBuffer(t);
		}
		// if (bufArray.length > 0)
		// System.out.println(String.format("From LocalBuffer: %s - %d",
		// t, bufArray.length));
		inputDataBuilder.put(t, ImmutableList.copyOf(bufArray));
	}

	private SNDrainedData getEmptyDrainData() {
		drainState = 5;
		ImmutableMap.Builder<Token, ImmutableList<Object>> inputDataBuilder = new ImmutableMap.Builder<>();
		ImmutableMap.Builder<Token, ImmutableList<Object>> outputDataBuilder = new ImmutableMap.Builder<>();
		ImmutableMap.Builder<Token, ImmutableList<Object>> dataBuilder = ImmutableMap
				.builder();
		ImmutableTable.Builder<Integer, String, Object> stateBuilder = ImmutableTable
				.builder();
		DrainData dd = new DrainData(dataBuilder.build(), stateBuilder.build());
		return new SNDrainElement.SNDrainedData(blobID, dd,
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

	private void printDrainDataStats(DrainData dd) {
		System.out.println("**********printDrainDataStats*************");
		if (dd != null) {
			for (Token t : dd.getData().keySet()) {
				int size = dd.getData().get(t).size();
				if (size > 0)
					System.out.println("From Blob: " + t.toString() + " - "
							+ size);
			}
		}
	}

	void start() {
		outChnlManager.waitToStart();
		inChnlManager.waitToStart();

		bufferMap = buildBufferMap();
		blob.installBuffers(bufferMap);

		for (Thread t : blobThreads)
			t.start();

		System.out.println(blobID + " started");
	}

	void startChannels() {
		outChnlManager.start();
		inChnlManager.start();
	}

	void stop() {
		inChnlManager.stop(DrainType.FINAL);
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

		if (this.blobsManagerImpl.monBufs != null)
			this.blobsManagerImpl.monBufs.stopMonitoring();
	}

	final class BlobThread2 extends Thread {

		private final BlobExecuter be;

		private final Runnable coreCode;

		private volatile boolean stopping = false;

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
							blobsManagerImpl.streamNode.controllerConnection
									.writeObject(AppStatus.ERROR);
						} catch (IOException e1) {
							e1.printStackTrace();
						}
					}
				}
			}
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
				blobsManagerImpl.streamNode.controllerConnection
						.writeObject(new SNTimeInfo.DrainingTime(
								blobExec.blobID, time));
			} catch (IOException e) {
				e.printStackTrace();
			}
			blobExec.drained();
		}
	}
}
