package edu.mit.streamjit.impl.distributed.runtimer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import edu.mit.streamjit.impl.distributed.common.Utils;

/**
 * Logs the opentuner's method call times for debugging purpose.
 * 
 * @author sumanan
 * @since 6 Mar, 2015
 */
public interface MethodTimeLogger {

	void bStartTuner();
	void eStartTuner();

	void bHandleTermination();
	void eHandleTermination();

	void bNewCfg();
	void eNewCfg(int round);

	void bReconfigure();
	void eReconfigure();

	void bTuningFinished();
	void eTuningFinished();

	void bTerminate();
	void eTerminate();

	void bIntermediateDraining();
	void eIntermediateDraining();

	void bManagerReconfigure();
	void eManagerReconfigure();

	void bGetFixedOutputTime();
	void eGetFixedOutputTime();

	void bTuningRound();
	void eTuningRound();

	void bCfgManagerNewcfg();
	void eCfgManagerNewcfg();

	/**
	 * Logs nothing.
	 */
	public static class NoMethodTimeLogger implements MethodTimeLogger {

		@Override
		public void bStartTuner() {
		}

		@Override
		public void eStartTuner() {
		}

		@Override
		public void bHandleTermination() {
		}

		@Override
		public void eHandleTermination() {
		}

		@Override
		public void bNewCfg() {
		}

		@Override
		public void eNewCfg(int round) {
		}

		@Override
		public void bReconfigure() {
		}

		@Override
		public void eReconfigure() {
		}

		@Override
		public void bTuningFinished() {
		}

		@Override
		public void eTuningFinished() {
		}

		@Override
		public void bTerminate() {
		}

		@Override
		public void eTerminate() {
		}

		@Override
		public void bIntermediateDraining() {
		}

		@Override
		public void eIntermediateDraining() {
		}

		@Override
		public void bManagerReconfigure() {
		}

		@Override
		public void eManagerReconfigure() {
		}

		@Override
		public void bGetFixedOutputTime() {
		}

		@Override
		public void eGetFixedOutputTime() {
		}

		@Override
		public void bTuningRound() {
		}

		@Override
		public void eTuningRound() {
		}

		@Override
		public void bCfgManagerNewcfg() {
		}

		@Override
		public void eCfgManagerNewcfg() {
		}
	}

	public static class MethodTimeLoggerImpl implements MethodTimeLogger {

		private final OutputStreamWriter osWriter;

		private final Stopwatch startTuner;
		private final Stopwatch handleTermination;
		private final Stopwatch newCfg;
		private final Stopwatch reconfigure;
		private final Stopwatch tuningFinished;
		private final Stopwatch terminate;
		private final Stopwatch intermediateDraining;
		private final Stopwatch managerReconfigure;
		private final Stopwatch getFixedOutputTime;
		private final Stopwatch tuningRound;
		private final Stopwatch CfgManagerNewcfg;

		private RuntimeMXBean rb = ManagementFactory.getRuntimeMXBean();

		public MethodTimeLoggerImpl(OutputStream os) {
			this(getOSWriter(os));
		}

		public MethodTimeLoggerImpl(OutputStreamWriter osWriter) {
			this.osWriter = osWriter;
			this.startTuner = Stopwatch.createUnstarted();
			this.handleTermination = Stopwatch.createUnstarted();
			this.newCfg = Stopwatch.createUnstarted();
			this.reconfigure = Stopwatch.createUnstarted();
			this.tuningFinished = Stopwatch.createUnstarted();
			this.terminate = Stopwatch.createUnstarted();
			this.intermediateDraining = Stopwatch.createUnstarted();
			this.managerReconfigure = Stopwatch.createUnstarted();
			this.getFixedOutputTime = Stopwatch.createUnstarted();
			this.tuningRound = Stopwatch.createUnstarted();
			this.CfgManagerNewcfg = Stopwatch.createUnstarted();
			write("Method\t\t\tUptime\t\telapsedtime\n");
			write("====================================================\n");
		}

		@Override
		public void bStartTuner() {
			begin(startTuner);
		}

		@Override
		public void eStartTuner() {
			end(startTuner, "startTuner");
		}

		@Override
		public void bHandleTermination() {
			begin(handleTermination);
		}

		@Override
		public void eHandleTermination() {
			end(handleTermination, "handleTermination");
		}

		@Override
		public void bNewCfg() {
			begin(newCfg);
		}

		@Override
		public void eNewCfg(int round) {
			end(newCfg, String.format("newCfg-%d", round));
		}

		@Override
		public void bReconfigure() {
			begin(reconfigure);
		}

		@Override
		public void eReconfigure() {
			end(reconfigure, "reconfigure");
		}

		@Override
		public void bTuningFinished() {
			begin(tuningFinished);
		}

		@Override
		public void eTuningFinished() {
			end(tuningFinished, "tuningFinished");
		}

		@Override
		public void bTerminate() {
			begin(terminate);
		}

		@Override
		public void eTerminate() {
			end(terminate, "terminate");
		}

		@Override
		public void bIntermediateDraining() {
			begin(intermediateDraining);
		}

		@Override
		public void eIntermediateDraining() {
			end(intermediateDraining, "intermediateDraining");
		}

		@Override
		public void bManagerReconfigure() {
			begin(managerReconfigure);
		}

		@Override
		public void eManagerReconfigure() {
			end(managerReconfigure, "managerReconfigure");
		}

		@Override
		public void bGetFixedOutputTime() {
			begin(getFixedOutputTime);
		}

		@Override
		public void eGetFixedOutputTime() {
			end(getFixedOutputTime, "getFixedOutputTime");
		}

		@Override
		public void bTuningRound() {
			begin(tuningRound);
		}

		@Override
		public void eTuningRound() {
			end(tuningRound, "tuningRound");
			write("--------------------------------------------------\n");
		}

		@Override
		public void bCfgManagerNewcfg() {
			begin(CfgManagerNewcfg);
		}

		@Override
		public void eCfgManagerNewcfg() {
			end(CfgManagerNewcfg, "CfgManagerNewcfg");
		}

		private void begin(Stopwatch sw) {
			sw.reset();
			sw.start();
		}

		private void end(Stopwatch sw, String methodName) {
			sw.stop();
			long uptime = rb.getUptime();
			long elapsedtime = sw.elapsed(TimeUnit.MILLISECONDS);
			write(String.format("%-22s\t%-12d\t%d\n", methodName, uptime,
					elapsedtime));
		}

		private void write(String msg) {
			if (osWriter != null)
				try {
					osWriter.write(msg);
					osWriter.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}

		private static OutputStreamWriter getOSWriter(OutputStream os) {
			if (os == null)
				return null;
			return new OutputStreamWriter(os);
		}
	}

	/**
	 * Writes the method call time info to appName/onlineTuner.txt file.
	 */
	public static class FileMethodTimeLogger extends MethodTimeLoggerImpl {
		public FileMethodTimeLogger(String appName) {
			super(Utils.fileWriter(appName, "onlineTuner.txt"));
		}
	}

	/**
	 * Prints the method call time info to the standard out.
	 */
	public static class PrintMethodTimeLogger extends MethodTimeLoggerImpl {
		public PrintMethodTimeLogger() {
			super(System.out);
		}
	}
}
