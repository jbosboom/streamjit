package edu.mit.streamjit.impl.distributed.profiler;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.impl.distributed.profiler.SNProfileElement.SNBufferStatusData;
import edu.mit.streamjit.impl.distributed.profiler.SNProfileElement.SNBufferStatusData.BlobBufferStatus;
import edu.mit.streamjit.impl.distributed.profiler.SNProfileElement.SNBufferStatusData.BufferStatus;

/**
 * Collection of various {@link ProfileElementLogger} implementations.
 * 
 * @author sumanan
 * @since 29 Jan, 2015
 */
public class ProfileElementLoggers {

	public static class FileProfileElementLogger
			extends
				ProfileElementLoggerImpl {

		public FileProfileElementLogger(String appName) {
			super(Utils.fileWriter(String.format("%s%sprofile.txt", appName,
					File.separator)));
		}
	}

	/**
	 * Prints the SNProfileElements to the StdOut.
	 * 
	 */
	public static class PrintProfileElementLogger
			extends
				ProfileElementLoggerImpl {
		public PrintProfileElementLogger() {
			super(System.out);
		}
	}

	private static class ProfileElementLoggerImpl implements
			ProfileElementLogger {

		private final OutputStreamWriter writer;

		private final Object lock = new Object();

		ProfileElementLoggerImpl(OutputStream writer) {
			this(getOSWriter(writer));
		}

		ProfileElementLoggerImpl(OutputStreamWriter writer) {
			this.writer = writer;
		}

		@Override
		public void process(SNBufferStatusData bufferStatusData) {
			if (writer == null)
				return;

			synchronized (lock) {
				try {
					writer.write(String.format("MachineID=%d\n",
							bufferStatusData.machineID));
					for (BlobBufferStatus bbs : bufferStatusData.blobsBufferStatusSet) {
						writer.write(String.format("\tBlob=%s\n", bbs.blobID));
						writer.write("\t\tInput...\n");
						for (BufferStatus bs : bbs.inputSet)
							writer.write(String.format("\t\t\t%s\n", bs));
						writer.write("\t\tOutput...\n");
						for (BufferStatus bs : bbs.outputSet)
							writer.write(String.format("\t\t\t%s\n", bs));
						writer.flush();
					}

				} catch (IOException ex) {
				}
			}
		}

		private static OutputStreamWriter getOSWriter(OutputStream os) {
			if (os == null)
				return null;
			return new OutputStreamWriter(os);
		}

		@Override
		public void newConfiguration(String cfgName) {
			synchronized (lock) {
				try {
					writer.write(String
							.format("--------------------------------%s--------------------------------\n",
									cfgName));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
