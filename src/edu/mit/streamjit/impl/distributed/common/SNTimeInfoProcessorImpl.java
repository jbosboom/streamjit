package edu.mit.streamjit.impl.distributed.common;

import java.io.FileWriter;
import java.io.IOException;

import edu.mit.streamjit.impl.distributed.StreamJitAppManager;
import edu.mit.streamjit.impl.distributed.common.SNTimeInfo.CompilationTime;
import edu.mit.streamjit.impl.distributed.common.SNTimeInfo.SNTimeInfoProcessor;

public class SNTimeInfoProcessorImpl implements SNTimeInfoProcessor {

	private final FileWriter compileTimeWriter;

	public SNTimeInfoProcessorImpl() {
		compileTimeWriter = getFileWriter("CompileTime.txt");
	}

	private FileWriter getFileWriter(String name) {
		FileWriter fw = null;
		try {
			fw = new FileWriter(name);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return fw;
	}

	/**
	 * {@link StreamJitAppManager} can call this at every reconfiguration to
	 * update time info log files.
	 * 
	 * @param reconfigNo
	 */
	public void reconfigNO(int reconfigNo) {
		if (compileTimeWriter != null) {
			try {
				compileTimeWriter
						.write(String
								.format("-------------------------%d-------------------------\n",
										reconfigNo));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * The total compile time from {@link StreamJitAppManager} point.
	 * 
	 * @param time
	 */
	public void totalCompileTime(long time) {
		if (compileTimeWriter != null) {
			try {
				compileTimeWriter.write(String.format(
						"Total compile time %dms\n", time));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void process(CompilationTime compilationTime) {
		if (compilationTime != null) {
			String msg = String.format("Blob-%s-%.0fms\n",
					compilationTime.blobID, compilationTime.milliSec);
			try {
				compileTimeWriter.write(msg);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}