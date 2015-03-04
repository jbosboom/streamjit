package edu.mit.streamjit.impl.distributed;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import edu.mit.streamjit.impl.common.TimeLogger;
import edu.mit.streamjit.impl.distributed.common.Utils;

/**
 * Collection of various {@link TimeLogger} implementations.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Nov 24, 2014
 * 
 */
public class TimeLoggers {

	/**
	 * Creates three files named compileTime.txt, runTime.txt and drainTime.txt
	 * inside app.name directory, and logs the time information.
	 * 
	 * @author sumanan
	 * @since Nov 25, 2014
	 */
	public static class FileTimeLogger extends TimeLoggerImpl {

		public FileTimeLogger(String appName) {
			super(Utils.fileWriter(appName, "compileTime.txt"), Utils
					.fileWriter(appName, "runTime.txt"), Utils.fileWriter(
					appName, "drainTime.txt"), Utils.fileWriter(appName,
					"searchTime.txt"));
		}
	}

	/**
	 * Logs nothing.
	 */
	public static class NoTimeLogger implements TimeLogger {

		@Override
		public void compilationFinished(boolean isCompiled, String msg) {
		}

		@Override
		public void compilationStarted() {

		}

		@Override
		public void drainingFinished(String msg) {
		}

		@Override
		public void drainingStarted() {
		}

		@Override
		public void logCompileTime(long time) {
		}

		@Override
		public void logCompileTime(String msg) {
		}

		@Override
		public void logDrainDataCollectionTime(long time) {
		}

		@Override
		public void logDrainTime(long time) {
		}

		@Override
		public void logDrainTime(String msg) {
		}

		@Override
		public void logRunTime(long time) {
		}

		@Override
		public void logRunTime(String msg) {
		}

		@Override
		public void newConfiguration(String cfgPrefix) {
		}

		@Override
		public void drainDataCollectionStarted() {
		}

		@Override
		public void drainDataCollectionFinished(String msg) {
		}

		@Override
		public void logSearchTime(long time) {
		}
	}

	/**
	 * Prints the values to the StdOut.
	 * 
	 */
	public static class PrintTimeLogger extends TimeLoggerImpl {

		public PrintTimeLogger() {
			super(System.out, System.out, System.out, System.out);
		}
	}

	private static class TimeLoggerImpl implements TimeLogger {

		private final OutputStreamWriter compileTimeWriter;

		private final OutputStreamWriter drainTimeWriter;

		private final OutputStreamWriter runTimeWriter;

		private final OutputStreamWriter searchTimeWriter;

		private int reconfigNo = 0;

		private Stopwatch compileTimeSW = null;

		private Stopwatch drainTimeSW = null;

		private Stopwatch drainDataCollectionTimeSW = null;

		private Stopwatch tuningRoundSW = null;

		TimeLoggerImpl(OutputStream compileOS, OutputStream runOs,
				OutputStream drainOs, OutputStream searchOs) {
			this(getOSWriter(compileOS), getOSWriter(runOs),
					getOSWriter(drainOs), getOSWriter(searchOs));
		}

		TimeLoggerImpl(OutputStreamWriter compileW, OutputStreamWriter runW,
				OutputStreamWriter drainW, OutputStreamWriter searchW) {
			compileTimeWriter = compileW;
			runTimeWriter = runW;
			drainTimeWriter = drainW;
			searchTimeWriter = searchW;
		}

		@Override
		public void compilationFinished(boolean isCompiled, String msg) {
			if (compileTimeSW != null) {
				compileTimeSW.stop();
				long time = compileTimeSW.elapsed(TimeUnit.MILLISECONDS);
				logCompileTime(time);
			}
		}

		@Override
		public void compilationStarted() {
			compileTimeSW = Stopwatch.createStarted();
		}

		@Override
		public void drainingFinished(String msg) {
			if (drainTimeSW != null && drainTimeSW.isRunning()) {
				drainTimeSW.stop();
				long time = drainTimeSW.elapsed(TimeUnit.MILLISECONDS);
				logDrainTime(time);
			}
		}

		@Override
		public void drainingStarted() {
			drainTimeSW = Stopwatch.createStarted();
		}

		@Override
		public void drainDataCollectionStarted() {
			drainDataCollectionTimeSW = Stopwatch.createStarted();
		}

		@Override
		public void drainDataCollectionFinished(String msg) {
			if (drainDataCollectionTimeSW != null) {
				drainDataCollectionTimeSW.stop();
				long time = drainDataCollectionTimeSW
						.elapsed(TimeUnit.MILLISECONDS);
				logDrainDataCollectionTime(time);
			}
		}

		@Override
		public void logCompileTime(long time) {
			write(compileTimeWriter,
					String.format("Total compile time %dms\n", time));
		}

		@Override
		public void logCompileTime(String msg) {
			write(compileTimeWriter, msg);
		}

		@Override
		public void logDrainDataCollectionTime(long time) {
			write(drainTimeWriter,
					String.format("Drain data collection time is %dms\n", time));
		}

		@Override
		public void logDrainTime(long time) {
			write(drainTimeWriter, String.format("Drain time is %dms\n", time));
		}

		@Override
		public void logDrainTime(String msg) {
			write(drainTimeWriter, msg);
		}

		@Override
		public void logRunTime(long time) {
			write(runTimeWriter,
					String.format("Execution time is %dms\n", time));
		}

		@Override
		public void logRunTime(String msg) {
			write(runTimeWriter, msg);
		}

		/**
		 * [24-02-2015] When a new configuration come from the OpenTuner, we
		 * drain previous configuration. So the drainTime file should be updated
		 * with previous configuration prefix.
		 */
		String prevcfgPrefix = "";

		@Override
		public void newConfiguration(String cfgPrefix) {
			reconfigNo++;
			if (cfgPrefix == null || cfgPrefix.isEmpty())
				cfgPrefix = new Integer(reconfigNo).toString();

			updateTuningRoundTime();

			String msg = String
					.format("----------------------------%s----------------------------\n",
							cfgPrefix);
			String msg1 = String
					.format("----------------------------%s----------------------------\n",
							prevcfgPrefix);
			write(compileTimeWriter, msg);
			write(runTimeWriter, msg);
			write(searchTimeWriter, msg);
			write(drainTimeWriter, msg1);
			prevcfgPrefix = cfgPrefix;
		}

		private void updateTuningRoundTime() {
			long time = 0;
			if (tuningRoundSW == null)
				tuningRoundSW = Stopwatch.createStarted();
			else {
				tuningRoundSW.stop();
				time = tuningRoundSW.elapsed(TimeUnit.SECONDS);
				tuningRoundSW.reset();
				tuningRoundSW.start();
				write(runTimeWriter,
						String.format("Tuning round time %dS\n", time));
			}
		}

		private static OutputStreamWriter getOSWriter(OutputStream os) {
			if (os == null)
				return null;
			return new OutputStreamWriter(os);
		}

		private void write(OutputStreamWriter osWriter, String msg) {
			if (osWriter != null) {
				try {
					osWriter.write(msg);
					osWriter.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		@Override
		public void logSearchTime(long time) {
			write(searchTimeWriter,
					String.format("Search time is %dms\n", time));
		}
	}
}
