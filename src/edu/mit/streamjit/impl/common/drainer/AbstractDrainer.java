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
package edu.mit.streamjit.impl.common.drainer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Sets;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.TimeLogger;
import edu.mit.streamjit.impl.common.drainer.BlobGraph.BlobNode;
import edu.mit.streamjit.impl.concurrent.ConcurrentStreamCompiler;
import edu.mit.streamjit.impl.distributed.DistributedStreamCompiler;
import edu.mit.streamjit.impl.distributed.StreamJitApp;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.DrainType;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.SNDrainedData;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.tuner.OnlineTuner;

/**
 * Abstract drainer is to perform draining on a stream application. Both
 * {@link DistributedStreamCompiler} and {@link ConcurrentStreamCompiler} may
 * extends this to implement the draining on their particular context. Works
 * coupled with {@link BlobNode} and {@link BlobGraph}.
 * 
 * <p>
 * Three type of draining could be carried out.
 * <ol>
 * <li>Intermediate draining: In this case, no data from input buffer will be
 * consumed and StreamJit app will not be stopped. Rather, StreamJit app will be
 * just paused for reconfiguration purpose. This draining may be triggered by
 * {@link OnlineTuner}.</li>
 * <li>Semi final draining: In this case, no data from input buffer will be
 * consumed but StreamJit app will be stopped. i.e, StreamJit app will be
 * stopped safely without consuming any new input. This draining may be
 * triggered by {@link OnlineTuner} after opentuner finish tuning and send it's
 * final configuration.</li>
 * <li>Final draining: At the end of input data. After this draining StreamJit
 * app will stop. This draining may be triggered by a {@link Input} when it run
 * out of input data.</li>
 * </ol>
 * </p>
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Jul 30, 2013
 */
public abstract class AbstractDrainer {

	/**
	 * This is added for debugging purpose. Just logs the size of the drain data
	 * on each channel for every draining. Calling
	 * AbstractDrainer#dumpDraindataStatistics() will write down the statistics
	 * into a file. This map and all related lines may be removed after system
	 * got stable.
	 */
	private Map<Token, List<Integer>> drainDataStatistics = null;

	/**
	 * Blob graph of the stream application that needs to be drained.
	 */
	protected BlobGraph blobGraph;

	/**
	 * Latch to block the external thread that calls
	 * {@link CompiledStream#awaitDrained()}.
	 */
	private final CountDownLatch finalLatch;

	/**
	 * Blocks the online tuner thread until drainer gets all drained data.
	 */
	private CountDownLatch drainDataLatch;

	private AtomicInteger noOfDrainData;

	/**
	 * Latch to block online tuner thread until intermediate draining is
	 * accomplished.
	 */
	private CountDownLatch intermediateLatch;

	private AtomicInteger unDrainedNodes;

	ScheduledExecutorService schExecutorService;

	/**
	 * State of the drainer.
	 */
	DrainerState state;

	private final TimeLogger logger;

	private final StreamJitApp<?, ?> app;

	public AbstractDrainer(StreamJitApp<?, ?> app, TimeLogger logger) {
		state = DrainerState.NODRAINING;
		finalLatch = new CountDownLatch(1);
		this.app = app;
		this.logger = logger;
	}

	/**
	 * Sets the blobGraph that is in execution. When
	 * {@link #startDraining(boolean)} is called, abstract drainer will traverse
	 * through the blobgraph and drain the stream application.
	 * 
	 * @param blobGraph
	 */
	public final void setBlobGraph(BlobGraph blobGraph) {
		if (state == DrainerState.NODRAINING) {
			this.blobGraph = blobGraph;
			unDrainedNodes = new AtomicInteger(blobGraph.getBlobIds().size());
			noOfDrainData = new AtomicInteger(blobGraph.getBlobIds().size());
			blobGraph.setDrainer(this);
		} else {
			throw new RuntimeException("Drainer is in draing mode.");
		}
	}

	/**
	 * Initiate the draining of the blobgraph. Three type of draining could be
	 * carried out.
	 * <ol>
	 * <li>type 0 - Intermediate draining: In this case, no new data from
	 * {@link Input} will be consumed and StreamJit app will not be stopped.
	 * Rather, StreamJit app will be just paused for reconfiguration purpose.
	 * This draining may be triggered by {@link OnlineTuner}.</li>
	 * <li>type 1 - Semi final draining: In this case, StreamJit app will be
	 * stopped safely without consuming any new data from {@link Input}. This
	 * draining may be triggered by {@link OnlineTuner} after opentuner finish
	 * tuning and send it's final configuration.</li>
	 * <li>type 2 - Final draining: At the end of input data. After this
	 * draining StreamJit app will stop. This draining may be triggered by a
	 * {@link Input} when it run out of input data.</li>
	 * </ol>
	 * 
	 * @param type
	 *            whether the draining is the final draining or intermediate
	 *            draining.
	 * @return true iff draining process has been started. startDraining will
	 *         fail if the final draining has already been called.
	 */
	public final boolean startDraining(int type) {
		if (state == DrainerState.NODRAINING) {
			boolean isFinal = false;
			switch (type) {
				case 0 :
					this.state = DrainerState.INTERMEDIATE;
					break;
				case 1 :
					this.state = DrainerState.FINAL;
					break;
				case 2 :
					this.state = DrainerState.FINAL;
					isFinal = true;
					break;
				default :
					throw new IllegalArgumentException(
							"Invalid draining type. type can be 0, 1, or 2.");
			}

			this.blobGraph.clearDrainData();
			drainDataLatch = new CountDownLatch(1);
			intermediateLatch = new CountDownLatch(1);

			try {
				prepareDraining(isFinal);
			} catch (Exception e) {
				this.state = DrainerState.NODRAINING;
				System.err
						.println("No Drain called. Exception in prepareDraining()");
				throw e;
			}

			if (Options.needDrainDeadlockHandler)
				this.schExecutorService = Executors
						.newSingleThreadScheduledExecutor();

			blobGraph.getSourceBlobNode().drain();

			return true;
		} else if (state == DrainerState.FINAL) {
			return false;
		} else {
			throw new RuntimeException("Drainer is in draining mode.");
		}
	}

	public boolean drainIntermediate() {
		logger.drainingStarted();
		boolean state = startDraining(0);
		if (!state) {
			String msg = "Final drain has already been called. No more intermediate draining.";
			System.err.println(msg);
			logger.drainingFinished(msg);
			return false;
		}

		System.err.println("awaitDrainedIntrmdiate");
		try {
			awaitDrainedIntrmdiate();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		drainingDone(this.state == DrainerState.FINAL);
		logger.drainingFinished("Intermediate");
		logger.drainDataCollectionStarted();
		try {
			awaitDrainData();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		logger.drainDataCollectionFinished("");
		return true;
	}

	public boolean drainFinal(Boolean isSemeFinal) {
		int drainType = 2;
		if (isSemeFinal)
			drainType = 1;
		logger.drainingStarted();
		boolean state = startDraining(drainType);
		if (!state) {
			return false;
		}

		System.err.println("awaitDrainedIntrmdiate");
		try {
			awaitDrainedIntrmdiate();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		drainingDone(false);
		logger.drainingFinished("Intermediate");
		logger.drainDataCollectionStarted();
		try {
			awaitDrainData();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		logger.drainDataCollectionFinished("");
		// TODO : Even after the final draining, we can clear some more
		// intermediate data by running an Interpreter blob.
		// StreamJitApp.minimizeDrainData() does this job. Uncomment the
		// following lines later.
		// app.drainData = app.minimizeDrainData(app.drainData);
		// printDrainDataStats(app.drainData);
		// dumpDrainData(app.drainData);
		drainingDone(true);
		stop();
		return true;
	}

	/**
	 * Once draining of a blob is done, it has to inform to the drainer by
	 * calling this method.
	 */
	public final void drained(Token blobID) {
		blobGraph.getBlobNode(blobID).drained();
	}

	/**
	 * Awaits for {@link DrainData} from all {@link StreamNode}s, combines the
	 * all received DrainData and set the combined DrainData to
	 * {@link StreamJitApp#drainData}.
	 * 
	 * @throws InterruptedException
	 */
	public final void awaitDrainData() throws InterruptedException {
		if (Options.useDrainData) {
			drainDataLatch.await();
			app.drainData = getDrainData();
		}
	}

	public final void newSNDrainData(SNDrainedData snDrainedData) {
		blobGraph.getBlobNode(snDrainedData.blobID).setDrainData(snDrainedData);
		if (noOfDrainData.decrementAndGet() == 0) {
			assert state == DrainerState.NODRAINING;
			drainDataLatch.countDown();
		}
	}

	// TODO: Too many unnecessary data copies are taking place at here, inside
	// the DrainData constructor and DrainData.merge(). Need to optimise these
	// all.
	/**
	 * @return Aggregated DrainData after the draining.
	 */
	private final DrainData getDrainData() {
		if (!Options.useDrainData)
			return null;
		DrainData drainData = null;
		Map<Token, ImmutableList<Object>> boundaryInputData = new HashMap<>();
		Map<Token, ImmutableList<Object>> boundaryOutputData = new HashMap<>();

		for (BlobNode node : blobGraph.blobNodes.values()) {
			boundaryInputData.putAll(node.snDrainData.inputData);
			boundaryOutputData.putAll(node.snDrainData.outputData);
			if (drainData == null)
				drainData = node.snDrainData.drainData;
			else
				drainData = drainData.merge(node.snDrainData.drainData);
		}

		ImmutableMap.Builder<Token, ImmutableList<Object>> dataBuilder = ImmutableMap
				.builder();
		for (Token t : Sets.union(boundaryInputData.keySet(),
				boundaryOutputData.keySet())) {
			ImmutableList<Object> in = boundaryInputData.get(t) != null
					? boundaryInputData.get(t) : ImmutableList.of();
			ImmutableList<Object> out = boundaryOutputData.get(t) != null
					? boundaryOutputData.get(t) : ImmutableList.of();
			dataBuilder.put(t, ImmutableList.builder().addAll(in).addAll(out)
					.build());
		}

		ImmutableTable<Integer, String, Object> state = ImmutableTable.of();
		DrainData draindata1 = new DrainData(dataBuilder.build(), state);
		drainData = drainData.merge(draindata1);
		updateDrainDataStatistics(drainData);
		// printDrainDataStats(drainData);
		// dumpDrainData(drainData);
		return drainData;
	}

	private void updateDrainDataStatistics(DrainData drainData) {
		if (drainDataStatistics == null) {
			drainDataStatistics = new HashMap<>();
			for (Token t : drainData.getData().keySet()) {
				drainDataStatistics.put(t, new ArrayList<Integer>());
			}
		}
		for (Token t : drainData.getData().keySet()) {
			int size = drainData.getData().get(t).size();
			drainDataStatistics.get(t).add(size);

		}
	}

	private void printDrainDataStats(DrainData drainData) {
		try {
			String fileName = String.format("%s%sdraindatasize.txt", app.name,
					File.separator);
			FileWriter writer = new FileWriter(fileName, true);
			writer.write("-----------------------------------------------------------\n");
			System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");

			for (Token t : drainData.getData().keySet()) {
				int size = drainData.getData().get(t).size();
				if (size != 0) {
					String msg = String.format("%s - %d\n", t.toString(), size);
					System.out.print(msg);
					writer.write(msg);
				}
			}
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void dumpDrainData(DrainData drainData) {
		try {
			String fileName = String.format("%s%sDrainData", app.name,
					File.separator);
			FileOutputStream fout = new FileOutputStream(fileName);
			ObjectOutputStream oos = new ObjectOutputStream(fout);
			oos.writeObject(drainData);
			oos.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * logs the size of the drain data on each channel for every draining and
	 * writes down the statistics into a file.
	 * 
	 * @throws IOException
	 */
	public void dumpDraindataStatistics() throws IOException {
		if (drainDataStatistics == null) {
			System.err.println("drainDataStatistics is null");
			return;
		}

		String fileName = String.format("%s%sDrainDataStatistics.txt",
				app.name, File.separator);
		FileWriter writer = new FileWriter(fileName);
		for (Token t : drainDataStatistics.keySet()) {
			writer.write(t.toString());
			writer.write(" - ");
			for (Integer i : drainDataStatistics.get(t)) {
				writer.write(i.toString() + '\n');
			}
			writer.write('\n');
		}
		writer.flush();
		writer.close();
	}

	/**
	 * @return true iff draining of the stream application is finished. See
	 *         {@link CompiledStream#isDrained()} for more details.
	 */
	public final boolean isDrained() {
		return finalLatch.getCount() == 0;
	}

	/**
	 * See {@link CompiledStream#awaitDrained()} for more details.
	 */
	public final void awaitDrained() throws InterruptedException {
		finalLatch.await();
	}

	public final void awaitDrainedIntrmdiate() throws InterruptedException {
		intermediateLatch.await();

		// The following while loop is added just for debugging purpose. To
		// activate the following while loop code snippet, comment the above
		// [intermediateLatch.await()] line.
		while (intermediateLatch.getCount() != 0) {
			Thread.sleep(3000);
			System.out.println("****************************************");
			for (BlobNode bn : blobGraph.blobNodes.values()) {
				switch (bn.drainState.get()) {
					case 0 :
						System.out.println(String.format("%s - No drain call",
								bn.blobID));
						break;
					case 1 :
						System.out.println(String.format(
								"%s - Drain requested", bn.blobID));
						break;
					case 2 :
						System.out
								.println(String
										.format("%s - Dead lock detected. Artificial drained has been called",
												bn.blobID));
						break;
					case 3 :
						System.out.println(String.format(
								"%s - Drain completed", bn.blobID));
						break;
					case 4 :
						System.out.println(String.format(
								"%s - DrainData Received", bn.blobID));
						break;
				}
			}
			System.out.println("****************************************");
		}
	}

	/**
	 * In any case, if the application could not be executed (may be due to
	 * {@link Error}), {@link StreamCompiler} or appropriate class can call this
	 * method to release the main thread.
	 */
	public void stop() {
		assert state != DrainerState.INTERMEDIATE : "DrainerState.NODRAINING or DrainerState.FINAL is expected.";
		this.finalLatch.countDown();
	}

	/**
	 * See {@link CompiledStream#awaitDrained(long, TimeUnit)} for more details.
	 */
	public final void awaitDrained(long timeout, TimeUnit unit)
			throws InterruptedException, TimeoutException {
		finalLatch.await(timeout, unit);
	}

	/**
	 * Once a {@link BlobNode}'s all preconditions are satisfied for draining,
	 * blob node will call this function drain the blob.
	 */
	protected abstract void drain(Token blobID, DrainType drainType);

	/**
	 * {@link AbstractDrainer} will call this function after the corresponding
	 * blob is drained. Sub classes may implement blob related resource cleanup
	 * jobs here ( e.g., stop blob threads).
	 * 
	 * @param blobID
	 * @param isFinal
	 *            : whether the draining is the final draining or intermediate
	 *            draining. Set to true for semi final case.
	 */
	protected abstract void drainingDone(Token blobID, boolean isFinal);

	/**
	 * {@link AbstractDrainer} will call this function after the draining
	 * process is complete. This can be used to do the final cleanups ( e.g, All
	 * data in the tail buffer should be consumed before this function returns.)
	 * After the return of this function, isDrained() will start to return true
	 * and any threads waiting at awaitdraining() will be released.
	 * 
	 * @param isFinal
	 *            : whether the draining is the final draining or intermediate
	 *            draining. Set to true for semi final case.
	 */
	protected abstract void drainingDone(boolean isFinal);

	/**
	 * {@link AbstractDrainer} will call this function as a first step to start
	 * a draining.
	 * 
	 * @param isFinal
	 *            :Whether the draining is the final draining or intermediate
	 *            draining. Set to false for semi final case.
	 */
	protected abstract void prepareDraining(boolean isFinal);

	/**
	 * {@link BlobNode}s have to call this function to inform draining done
	 * event.
	 * 
	 * @param blobNode
	 */
	void drainingDone(BlobNode blobNode) {
		assert state != DrainerState.NODRAINING : "Illegal call. Drainer is not in draining mode.";
		drainingDone(blobNode.blobID, state == DrainerState.FINAL);
		if (unDrainedNodes.decrementAndGet() == 0) {
			intermediateLatch.countDown();
			if (state == DrainerState.FINAL) {
			} else {
				state = DrainerState.NODRAINING;
			}

			if (Options.needDrainDeadlockHandler)
				schExecutorService.shutdownNow();
		}
	}

	/**
	 * Reflects {@link AbstractDrainer}'s state.
	 */
	enum DrainerState {
		NODRAINING, /**
		 * Draining in middle of the stream graph's execution. This
		 * type of draining will be triggered by the open tuner for
		 * reconfiguration. Drained data of all blobs are expected in this case.
		 */
		INTERMEDIATE, /**
		 * This type of draining will take place when input stream
		 * runs out. No drained data expected as all blob are expected to
		 * executes until all input buffers become empty.
		 */
		FINAL
	}
}
