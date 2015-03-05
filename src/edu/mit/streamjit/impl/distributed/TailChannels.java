package edu.mit.streamjit.impl.distributed;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
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
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.impl.distributed.common.Utils;
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
					Options.outputCount));

			try {
				writer.write(String.format("GlobalConstants.outputCount = %d",
						Options.outputCount));
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

		private final String appName;

		private final TailChannel tailChannel;

		/**
		 * The no of outputs received at the end of last period.
		 */
		private int lastCount;

		private FileWriter writer;

		private RuntimeMXBean rb = ManagementFactory.getRuntimeMXBean();

		private ScheduledExecutorService scheduledExecutorService;

		OutputCountPrinter(TailChannel tailChannel, String appName) {
			this.tailChannel = tailChannel;
			this.appName = appName;
			printOutputCount();
		}

		private void printOutputCount() {
			if (Options.printOutputCountPeriod < 1)
				return;
			writer = Utils.fileWriter(appName, "outputCount.txt", true);
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
							String msg = String.format("%d\t\t%d\t\t%d\n",
									rb.getUptime(), currentCount, newOutputs);
							try {
								writer.write(msg);
								writer.flush();
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}, Options.printOutputCountPeriod,
					Options.printOutputCountPeriod,
					TimeUnit.MILLISECONDS);
		}

		private void stop() {
			if (scheduledExecutorService != null)
				scheduledExecutorService.shutdown();
		}

		/**
		 * This method writes to the file in a non thread safe way. But this is
		 * enough to serve the purpose.
		 * <P>
		 * TODO: This method is just for debugging purpose, Remove this method
		 * and its usage later.
		 */
		private boolean write(String msg) {
			if (writer != null)
				try {
					writer.write(msg);
					return true;
				} catch (Exception e) {
				}
			return false;
		}
	}

	private static abstract class AbstractBlockingTailChannel
			extends
				BlockingInputChannel implements TailChannel {

		protected final int skipCount;

		protected final int totalCount;

		protected int count;

		private PerformanceLogger pLogger = null;

		private OutputCountPrinter outputCountPrinter = null;

		private final String cfgPrefix;

		protected abstract void releaseAndInitilize();

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
		public AbstractBlockingTailChannel(Buffer buffer,
				ConnectionProvider conProvider, ConnectionInfo conInfo,
				String bufferTokenName, int debugLevel, int skipCount,
				int steadyCount, String appName, String cfgPrefix) {
			super(buffer, conProvider, conInfo, bufferTokenName, debugLevel);
			this.skipCount = skipCount;
			this.totalCount = steadyCount + skipCount;
			count = 0;
			this.cfgPrefix = cfgPrefix;
			if (Options.tune == 0) {
				// TODO: Leaks this object from the constructor. May cause
				// subtle bugs. Re-factor it.
				pLogger = new PerformanceLogger(this, appName);
				pLogger.start();
			}
			if (Options.printOutputCountPeriod > 0)
				outputCountPrinter = new OutputCountPrinter(this, appName);
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

		@Override
		public int count() {
			return count;
		}

		protected long normalizedTime(long time) {
			return (Options.outputCount * time) / (totalCount - skipCount);
		}

		/**
		 * Opposite to the {@link #normalizedTime(long)}'s equation.
		 * <code>time=unnormalizedTime(normalizedTime(time))</code>
		 */
		protected long unnormalizedTime(long time) {
			return (time * (totalCount - skipCount)) / Options.outputCount;
		}

		/**
		 * Logs the time reporting event.
		 * 
		 * TODO: This method is just for debugging purpose, Remove this method
		 * and its usage later.
		 */
		protected void reportingTime(long time) {
			if (outputCountPrinter != null) {
				String msg = String.format("Reporting...%s.cfg,time=%d\n",
						cfgPrefix, time);
				outputCountPrinter.write(msg);
			}
		}
	}

	public static final class BlockingTailChannel1
			extends
				AbstractBlockingTailChannel {

		private volatile CountDownLatch steadyLatch;

		private volatile CountDownLatch skipLatch;

		private boolean skipLatchUp;

		private boolean steadyLatchUp;

		/**
		 * @param buffer
		 * @param conProvider
		 * @param conInfo
		 * @param bufferTokenName
		 * @param debugLevel
		 *            For all above 5 parameters, see
		 *            {@link BlockingInputChannel#BlockingInputChannel(Buffer, ConnectionProvider, ConnectionInfo, String, int)}
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
				int steadyCount, String appName, String cfgPrefix) {
			super(buffer, conProvider, conInfo, bufferTokenName, debugLevel,
					skipCount, steadyCount, appName, cfgPrefix);
			steadyLatch = new CountDownLatch(1);
			skipLatch = new CountDownLatch(1);
			this.skipLatchUp = true;
			this.steadyLatchUp = true;
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
			reportingTime(time);
			return normalizedTime(time);
		}

		@Override
		public long getFixedOutputTime(long timeout)
				throws InterruptedException {
			if (timeout < 1)
				return getFixedOutputTime();

			timeout = unnormalizedTime(timeout);
			releaseAndInitilize();
			skipLatch.await();
			Stopwatch stopwatch = Stopwatch.createStarted();
			while (steadyLatch.getCount() > 0
					&& stopwatch.elapsed(TimeUnit.MILLISECONDS) < timeout) {
				Thread.sleep(100);
			}

			stopwatch.stop();
			long time = stopwatch.elapsed(TimeUnit.MILLISECONDS);
			reportingTime(time);
			if (time > timeout)
				return -1;
			return normalizedTime(time);
		}

		/**
		 * Releases all latches, and re-initializes the latches and counters.
		 */
		protected void releaseAndInitilize() {
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
	}

	public static final class BlockingTailChannel2
			extends
				AbstractBlockingTailChannel {

		private volatile CountDownLatch skipLatch;

		private boolean skipLatchUp;

		private final Stopwatch stopWatch;

		/**
		 * @param buffer
		 * @param conProvider
		 * @param conInfo
		 * @param bufferTokenName
		 * @param debugLevel
		 *            For all above 5 parameters, see
		 *            {@link BlockingInputChannel#BlockingInputChannel(Buffer, ConnectionProvider, ConnectionInfo, String, int)}
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
		public BlockingTailChannel2(Buffer buffer,
				ConnectionProvider conProvider, ConnectionInfo conInfo,
				String bufferTokenName, int debugLevel, int skipCount,
				int steadyCount, String appName, String cfgPrefix) {
			super(buffer, conProvider, conInfo, bufferTokenName, debugLevel,
					skipCount, steadyCount, appName, cfgPrefix);
			stopWatch = Stopwatch.createUnstarted();
			skipLatch = new CountDownLatch(1);
			this.skipLatchUp = true;
		}

		@Override
		public void receiveData() {
			super.receiveData();
			count++;

			if (skipLatchUp && count > skipCount) {
				skipLatch.countDown();
				skipLatchUp = false;
			}

			if (stopWatch.isRunning() && count > totalCount) {
				stopWatch.stop();
			}
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
			stopWatch.start();
			while (stopWatch.isRunning())
				Thread.sleep(250);
			long time = stopWatch.elapsed(TimeUnit.MILLISECONDS);
			reportingTime(time);
			return normalizedTime(time);
		}

		@Override
		public long getFixedOutputTime(long timeout)
				throws InterruptedException {
			if (timeout < 1)
				return getFixedOutputTime();

			timeout = unnormalizedTime(timeout);
			releaseAndInitilize();
			skipLatch.await();
			stopWatch.start();
			while (stopWatch.isRunning()
					&& stopWatch.elapsed(TimeUnit.MILLISECONDS) < timeout) {
				Thread.sleep(250);
			}

			long time = stopWatch.elapsed(TimeUnit.MILLISECONDS);
			reportingTime(time);
			if (time > timeout)
				return -1;
			else
				return normalizedTime(time);
		}

		/**
		 * Releases all latches, and re-initializes the latches and counters.
		 */
		protected void releaseAndInitilize() {
			count = 0;
			skipLatch.countDown();
			skipLatch = new CountDownLatch(1);
			skipLatchUp = true;
			stopWatch.reset();
		}

		public void reset() {
			stopWatch.reset();
			skipLatch.countDown();
			count = 0;
		}
	}
}