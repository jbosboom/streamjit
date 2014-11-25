package edu.mit.streamjit.impl.distributed;

import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import edu.mit.streamjit.impl.common.TimeLogger;

/**
 * Collection of various {@link TimeLogger} implementations.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Nov 24, 2014
 * 
 */
public class TimeLoggers {

	/**
	 * Creates three files named prefix_compileTime.txt, prefix_runTime.txt and
	 * prefix_drainTime.txt, and logs the time information.
	 * 
	 * @author sumanan
	 * @since Nov 25, 2014
	 */
	public static class FileTimeLogger extends TimeLoggerImpl {

		private static FileWriter getFileWriter(String name) {
			FileWriter fw = null;
			try {
				fw = new FileWriter(name);
			} catch (IOException e) {
				e.printStackTrace();
			}
			return fw;
		}

		/**
		 * @param prefix
		 *            : prefix for the file names.
		 */
		public FileTimeLogger(String prefix) {
			super(getFileWriter(String.format("%s_compileTime.txt", prefix)),
					getFileWriter(String.format("%s_runTime.txt", prefix)),
					getFileWriter(String.format("%s_drainTime.txt", prefix)));
		}
	}

	/**
	 * Logs nothing.
	 */
	public static class NoTimeLogger implements TimeLogger {

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
		public void newReconfiguration() {
		}

	}

	/**
	 * Prints the values to the StdOut.
	 * 
	 */
	public static class PrintTimeLogger extends TimeLoggerImpl {

		public PrintTimeLogger() {
			super(System.out, System.out, System.out);
		}
	}

	private static class TimeLoggerImpl implements TimeLogger {

		private final OutputStreamWriter compileTimeWriter;

		private final OutputStreamWriter drainTimeWriter;

		private int reconfigNo = 0;

		private final OutputStreamWriter runTimeWriter;

		TimeLoggerImpl(OutputStream compileOS, OutputStream runOs,
				OutputStream drainOs) {
			compileTimeWriter = getOSWriter(compileOS);
			runTimeWriter = getOSWriter(runOs);
			drainTimeWriter = getOSWriter(drainOs);
		}

		TimeLoggerImpl(OutputStreamWriter compileW, OutputStreamWriter runW,
				OutputStreamWriter drainW) {
			compileTimeWriter = compileW;
			runTimeWriter = runW;
			drainTimeWriter = drainW;
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

		@Override
		public void newReconfiguration() {
			reconfigNo++;
			String msg = String
					.format("----------------------------%d----------------------------\n",
							reconfigNo);
			write(compileTimeWriter, msg);
			write(runTimeWriter, msg);
			write(drainTimeWriter, msg);
		}

		private OutputStreamWriter getOSWriter(OutputStream os) {
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
	}
}
