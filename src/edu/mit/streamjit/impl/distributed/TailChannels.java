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

public class TailChannels {

	private static class PerformanceLogger extends Thread {

		private AtomicBoolean stopFlag;

		private final String appName;

		private final TailChannel tailChannel;

		private PerformanceLogger(TailChannel tailChannel, String appName) {
			super("PerformanceLogger");
			stopFlag = new AtomicBoolean(false);
			this.appName = appName;
			this.tailChannel = tailChannel;
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
					Long time = tailChannel.getFixedOutputTime();

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

	/**
	 * Periodically prints the number of outputs received by a
	 * {@link TailChannel}.
	 */
	private static class OutputCountPrinter {

		private final TailChannel tailChannel;

		/**
		 * The no of outputs received at the end of last period.
		 */
		private int lastCount;

		private ScheduledExecutorService scheduledExecutorService;

		OutputCountPrinter(TailChannel tailChannel) {
			this.tailChannel = tailChannel;
			printOutputCount();
		}

		private void printOutputCount() {
			if (GlobalConstants.printOutputCountPeriod < 1)
				return;

			lastCount = 0;
			scheduledExecutorService = Executors
					.newSingleThreadScheduledExecutor();
			scheduledExecutorService.scheduleAtFixedRate(
					new Runnable() {

						@Override
						public void run() {
							int currentCount = tailChannel.count();
							int newOutputs = currentCount - lastCount;
							lastCount = currentCount;
							System.out.println(String
									.format("Outputs: since started - %d, during last %d ms - %d",
											currentCount,
											GlobalConstants.printOutputCountPeriod,
											newOutputs));

						}
					}, GlobalConstants.printOutputCountPeriod,
					GlobalConstants.printOutputCountPeriod,
					TimeUnit.MILLISECONDS);
		}

		private void stop() {
			if (scheduledExecutorService != null)
				scheduledExecutorService.shutdown();
		}
	}

	public static class BlockingTailChannel1 extends BlockingInputChannel
			implements TailChannel {

		private final int skipCount;

		private final int totalCount;

		private int count;

		private volatile CountDownLatch steadyLatch;

		private volatile CountDownLatch skipLatch;

		private PerformanceLogger pLogger = null;

		private OutputCountPrinter outputCountPrinter = null;

		private boolean skipLatchUp;

		private boolean steadyLatchUp;

		/**
		 * @param buffer
		 * @param conProvider
		 * @param conInfo
		 * @param bufferTokenName
		 * @param debugLevel
		 * @param skipCount
		 *            : Skips this amount of output before evaluating the
		 *            running time. This is added to avoid the noise from init
		 *            schedule and the drain data. ( i.e., In order to get real
		 *            steady state execution time)
		 * @param steadyCount
		 *            : {@link #getFixedOutputTime()} calculates the time taken
		 *            to get this amount of outputs ( after skipping skipCount
		 *            number of outputs at the beginning).
		 */
		public BlockingTailChannel1(Buffer buffer,
				ConnectionProvider conProvider, ConnectionInfo conInfo,
				String bufferTokenName, int debugLevel, int skipCount,
				int steadyCount, String appName) {
			super(buffer, conProvider, conInfo, bufferTokenName, debugLevel);
			this.skipCount = skipCount;
			this.totalCount = steadyCount + skipCount;
			count = 0;
			steadyLatch = new CountDownLatch(1);
			skipLatch = new CountDownLatch(1);
			this.skipLatchUp = true;
			this.steadyLatchUp = true;
			if (GlobalConstants.tune == 0) {
				// TODO: Leaks this object from the constructor. May cause
				// subtle bugs. Re-factor it.
				pLogger = new PerformanceLogger(this, appName);
				pLogger.start();
			}
			if (GlobalConstants.printOutputCountPeriod > 0)
				outputCountPrinter = new OutputCountPrinter(this);
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
				releaseAndInitilize();
				pLogger.stopLogging();
			}
			if (outputCountPrinter != null)
				outputCountPrinter.stop();
		}

		/**
		 * Skips skipCount amount of output at the beginning and then calculates
		 * the time taken to get steadyCount amount of outputs. skipCount is
		 * added to avoid the noise from init schedule and the drain data. (
		 * i.e., In order to get real steady state execution time).
		 * 
		 * @return time in MILLISECONDS.
		 * @throws InterruptedException
		 */
		public long getFixedOutputTime() throws InterruptedException {
			releaseAndInitilize();
			skipLatch.await();
			Stopwatch stopwatch = Stopwatch.createStarted();
			steadyLatch.await();
			stopwatch.stop();
			long time = stopwatch.elapsed(TimeUnit.MILLISECONDS);
			return normalizedTime(time);
		}

		@Override
		public long getFixedOutputTime(long timeout)
				throws InterruptedException {
			timeout = unnormalizedTime(timeout);
			releaseAndInitilize();
			skipLatch.await();
			Stopwatch stopwatch = Stopwatch.createStarted();
			while (steadyLatch.getCount() > 0
					&& stopwatch.elapsed(TimeUnit.MILLISECONDS) < timeout) {
				Thread.sleep(100);
			}

			if (stopwatch.elapsed(TimeUnit.MILLISECONDS) > timeout)
				return -1;

			stopwatch.stop();
			long time = stopwatch.elapsed(TimeUnit.MILLISECONDS);
			return normalizedTime(time);
		}

		/**
		 * Releases all latches, and re-initializes the latches and counters.
		 */
		private void releaseAndInitilize() {
			count = 0;
			skipLatch.countDown();
			skipLatch = new CountDownLatch(1);
			skipLatchUp = true;
			steadyLatch.countDown();
			steadyLatch = new CountDownLatch(1);
			steadyLatchUp = true;
		}

		public void reset() {
			steadyLatch.countDown();
			skipLatch.countDown();
			count = 0;
		}

		@Override
		public int count() {
			return count;
		}

		private long normalizedTime(long time) {
			return (GlobalConstants.outputCount * time)
					/ (totalCount - skipCount);
		}

		/**
		 * Opposite to the {@link #normalizedTime(long)}'s equation.
		 * <code>time=unnormalizedTime(normalizedTime(time))</code>
		 */
		private long unnormalizedTime(long time) {
			return (time * (totalCount - skipCount))
					/ GlobalConstants.outputCount;
		}
	}
}