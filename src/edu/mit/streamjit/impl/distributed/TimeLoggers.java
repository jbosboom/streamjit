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
	 * Logs nothing.
	 */
	public static class NoTimeLogger implements TimeLogger {

		@Override
		public void newReconfiguration() {
		}

		@Override
		public void logCompileTime(long time) {
		}

		@Override
		public void logRunTime(long time) {
		}

		@Override
		public void logDrainTime(long time) {
		}

		@Override
		public void logDrainDataCollectionTime(long time) {
		}

		@Override
		public void logCompileTime(String msg) {
		}

		@Override
		public void logDrainTime(String msg) {
		}

		@Override
		public void logRunTime(String msg) {
		}

	}

	private static class TimeLoggerImpl implements TimeLogger {

		private final OutputStreamWriter compileTimeWriter;

		private final OutputStreamWriter runTimeWriter;

		private final OutputStreamWriter drainTimeWriter;

		private int reconfigNo = 0;

		TimeLoggerImpl(OutputStreamWriter compileW, OutputStreamWriter runW,
				OutputStreamWriter drainW) {
			compileTimeWriter = compileW;
			runTimeWriter = runW;
			drainTimeWriter = drainW;
		}

		TimeLoggerImpl(OutputStream compileOS, OutputStream runOs,
				OutputStream drainOs) {
			compileTimeWriter = getOSWriter(compileOS);
			runTimeWriter = getOSWriter(runOs);
			drainTimeWriter = getOSWriter(drainOs);
		}

		private OutputStreamWriter getOSWriter(OutputStream os) {
			if (os == null)
				return null;
			return new OutputStreamWriter(os);
		}

		@Override
		public void newReconfiguration() {
			reconfigNo++;
			String msg = String
					.format("----------------------------%d----------------------------",
							reconfigNo);
			write(compileTimeWriter, msg);
			write(runTimeWriter, msg);
			write(drainTimeWriter, msg);
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
		public void logCompileTime(long time) {
			write(compileTimeWriter,
					String.format("Total compile time %dms\n", time));
		}

		@Override
		public void logRunTime(long time) {
			write(runTimeWriter,
					String.format("Execution time is %dms\n", time));
		}

		@Override
		public void logDrainTime(long time) {
			write(drainTimeWriter, String.format("Drain time is %dms\n", time));
		}

		@Override
		public void logDrainDataCollectionTime(long time) {
			write(drainTimeWriter,
					String.format("Drain data collection time is %dms\n", time));
		}

		@Override
		public void logCompileTime(String msg) {
			write(compileTimeWriter, msg);
		}

		@Override
		public void logDrainTime(String msg) {
			write(drainTimeWriter, msg);
		}

		@Override
		public void logRunTime(String msg) {
			write(runTimeWriter, msg);
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

	public static class FileTimeLogger extends TimeLoggerImpl {

		FileTimeLogger() {
			super(getFileWriter("compileTime.txt"),
					getFileWriter("runTime.txt"),
					getFileWriter("drainTime.txt"));
		}

		private static FileWriter getFileWriter(String name) {
			FileWriter fw = null;
			try {
				fw = new FileWriter(name);
			} catch (IOException e) {
				e.printStackTrace();
			}
			return fw;
		}
	}
}
