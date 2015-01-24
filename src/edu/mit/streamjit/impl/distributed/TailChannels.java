package edu.mit.streamjit.impl.distributed;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Stopwatch;

import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.DrainType;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionProvider;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.node.BlockingInputChannel;

public class TailChannels extends BlockingInputChannel {

	private final int skipCount;

	private final int totalCount;

	private int count;

	private volatile CountDownLatch steadyLatch;

	private volatile CountDownLatch skipLatch;

	private PerformanceLogger pLogger = null;

	private boolean skipLatchUp;

	private boolean steadyLatchUp;

	/**
	 * Periodically prints no of outputs generated. See
	 * {@link #printOutputCount()}.
	 */
	private ScheduledExecutorService scheduledExecutorService;

	/**
	 * The no of outputs generated at the end of last period. See
	 * {@link #printOutputCount()}.
	 */
	private int lastCount;

	/**
	 * @param buffer
	 * @param conProvider
	 * @param conInfo
	 * @param bufferTokenName
	 * @param debugLevel
	 * @param skipCount
	 *            : Skips this amount of output before evaluating the running
	 *            time. This is added to avoid the noise from init schedule and
	 *            the drain data. ( i.e., In order to get real steady state
	 *            execution time)
	 * @param steadyCount
	 *            : {@link #getFixedOutputTime()} calculates the time taken to
	 *            get this amount of outputs ( after skipping skipCount number
	 *            of outputs at the beginning).
	 */
	public TailChannels(Buffer buffer, ConnectionProvider conProvider,
			ConnectionInfo conInfo, String bufferTokenName, int debugLevel,
			int skipCount, int steadyCount, String appName) {
		super(buffer, conProvider, conInfo, bufferTokenName, debugLevel);
		this.skipCount = skipCount;
		this.totalCount = steadyCount + skipCount;
		count = 0;
		lastCount = 0;
		steadyLatch = new CountDownLatch(1);
		skipLatch = new CountDownLatch(1);
		this.skipLatchUp = true;
		this.steadyLatchUp = true;
		if (GlobalConstants.tune == 0) {
			pLogger = new PerformanceLogger(appName);
			pLogger.start();
		}
		printOutputCount();
	}

	@Override
	public void receiveData() {
		super.receiveData();
		count++;

		if (skipLatchUp && count > skipCount) {
			skipLatch.countDown();
			skipLatchUp = false;
		}

		if (steadyLatchUp && count > totalCount) {
			steadyLatch.countDown();
			steadyLatchUp = false;
		}
	}

	@Override
	public void stop(DrainType type) {
		super.stop(type);
		if (pLogger != null) {
			resetAll();
			pLogger.stopLogging();
		}
		if (scheduledExecutorService != null)
			scheduledExecutorService.shutdown();
	}

	/**
	 * Skips skipCount amount of output at the beginning and then calculates the
	 * time taken to get steadyCount amount of outputs. skipCount is added to
	 * avoid the noise from init schedule and the drain data. ( i.e., In order
	 * to get real steady state execution time).
	 * 
	 * @return time in MILLISECONDS.
	 * @throws InterruptedException
	 */
	public long getFixedOutputTime() throws InterruptedException {
		resetAll();
		skipLatch.await();
		Stopwatch stopwatch = Stopwatch.createStarted();
		steadyLatch.await();
		stopwatch.stop();
		long time = stopwatch.elapsed(TimeUnit.MILLISECONDS);
		long normalizedTime = (GlobalConstants.outputCount * time)
				/ (totalCount - skipCount);
		return normalizedTime;
	}

	/**
	 * Periodically prints no of outputs generated.
	 */
	private void printOutputCount() {
		if (GlobalConstants.printOutputCountPeriod < 1)
			return;

		lastCount = 0;
		scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
		scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				int newOutputs = count - lastCount;
				lastCount = count;
				System.out.println(String.format(
						"Outputs: since started - %d, during last %d ms - %d",
						count, GlobalConstants.printOutputCountPeriod,
						newOutputs));

			}
		}, GlobalConstants.printOutputCountPeriod,
				GlobalConstants.printOutputCountPeriod, TimeUnit.MILLISECONDS);
	}

	private void resetAll() {
		count = 0;
		lastCount = 0;
		skipLatch.countDown();
		skipLatch = new CountDownLatch(1);
		skipLatchUp = true;
		steadyLatch.countDown();
		steadyLatch = new CountDownLatch(1);
		steadyLatchUp = true;
	}

	/**
	 * We need this method apart from {@link #resetAll()}, because the
	 * {@link #resetAll()} method creates the latches immediately after
	 * countDown(). This causes the threads which are waiting on the latches at
	 * {@link #getFixedOutputTime()} will not be released properly.
	 */
	public void releaseAll() {
		steadyLatch.countDown();
		skipLatch.countDown();
		count = 0;
		lastCount = 0;
	}

	private class PerformanceLogger extends Thread {

		private AtomicBoolean stopFlag;

		private final String appName;

		private PerformanceLogger(String appName) {
			super("PerformanceLogger");
			stopFlag = new AtomicBoolean(false);
			this.appName = appName;
		}

		public void run() {
			int i = 0;
			FileWriter writer;
			try {
				writer = new FileWriter(String.format("%s%sFixedOutPut.txt",
						appName, File.separator));
			} catch (IOException e1) {
				e1.printStackTrace();
				return;
			}

			writeInitialInfo(writer);

			Long sum = 0l;

			while (++i < 10 && !stopFlag.get()) {
				try {
					Long time = getFixedOutputTime();

					sum += time;
					System.out.println("Execution time is " + time
							+ " milli seconds");

					writer.write(time.toString());
					writer.write('\n');
					writer.flush();
				} catch (InterruptedException | IOException e) {
					e.printStackTrace();
				}
			}
			try {
				writer.write("Average = " + sum / (i - 1));
				writer.write('\n');
				writer.flush();
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

			System.out.println("PerformanceLogger exits. App will run till "
					+ "inputdata exhausted.");
		}

		private void writeInitialInfo(FileWriter writer) {
			System.out.println(String.format(
					"PerformanceLogger starts to log the time to"
							+ " produce %d number of outputs",
					GlobalConstants.outputCount));

			try {
				writer.write(String.format("GlobalConstants.outputCount = %d",
						GlobalConstants.outputCount));
				writer.write('\n');
				writer.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void stopLogging() {
			stopFlag.set(true);
		}
	}
}