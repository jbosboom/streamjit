package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;
import java.util.HashSet;

import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.CTRLRDrainProcessor;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageVisitor;
import edu.mit.streamjit.impl.distributed.common.Command;
import edu.mit.streamjit.impl.distributed.common.Command.CommandProcessor;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString.ConfigurationProcessor;
import edu.mit.streamjit.impl.distributed.common.MiscCtrlElements;
import edu.mit.streamjit.impl.distributed.common.MiscCtrlElements.MiscCtrlElementProcessor;
import edu.mit.streamjit.impl.distributed.common.MiscCtrlElements.NewConInfo;
import edu.mit.streamjit.impl.distributed.common.NodeInfo;
import edu.mit.streamjit.impl.distributed.common.Request;
import edu.mit.streamjit.impl.distributed.common.Request.RequestProcessor;
import edu.mit.streamjit.impl.distributed.profiler.Profiler;
import edu.mit.streamjit.impl.distributed.profiler.ProfilerCommand;
import edu.mit.streamjit.impl.distributed.profiler.ProfilerCommand.ProfilerCommandProcessor;
import edu.mit.streamjit.impl.distributed.profiler.StreamNodeProfiler;

/**
 * @author Sumanan sumanan@mit.edu
 * @since May 20, 2013
 */
public class CTRLRMessageVisitorImpl implements CTRLRMessageVisitor {

	private final StreamNode streamNode;
	private final RequestProcessor rp;
	private final ConfigurationProcessor jp;
	private final MiscCtrlElementProcessor miscProcessor;
	private final ProfilerCommandProcessorImpl pm;

	public CTRLRMessageVisitorImpl(StreamNode streamNode) {
		this.streamNode = streamNode;
		this.rp = new RequestProcessorImpl();
		this.jp = new ConfigurationProcessorImpl(streamNode);
		this.miscProcessor = new MiscCtrlElementProcessorImpl();
		this.pm = new ProfilerCommandProcessorImpl();
	}

	@Override
	public void visit(Command streamJitCommand) {
		BlobsManager manager = streamNode.getBlobsManager();
		if (manager == null) {
			System.err.println("No AppStatusProcessor processor.");
			return;
		}
		CommandProcessor cp = manager.getCommandProcessor();
		streamJitCommand.process(cp);
	}

	@Override
	public void visit(Request request) {
		request.process(rp);
	}

	@Override
	public void visit(ConfigurationString json) {
		json.process(jp);
	}

	@Override
	public void visit(CTRLRDrainElement ctrlrDrainElement) {

		BlobsManager manager = streamNode.getBlobsManager();
		if (manager == null) {
			System.err.println("No AppStatusProcessor processor.");
			return;
		}
		CTRLRDrainProcessor dp = manager.getDrainProcessor();
		ctrlrDrainElement.process(dp);
	}

	@Override
	public void visit(MiscCtrlElements miscCtrlElements) {
		miscCtrlElements.process(miscProcessor);
	}

	@Override
	public void visit(ProfilerCommand command) {
		command.process(pm);
	}

	public class MiscCtrlElementProcessorImpl implements
			MiscCtrlElementProcessor {

		@Override
		public void process(NewConInfo newConInfo) {
			// TODO
			System.err.println("Need to process this soon");
		}
	}

	/**
	 * {@link RequestProcessor} at {@link StreamNode} side.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since May 27, 2013
	 */
	public class RequestProcessorImpl implements RequestProcessor {

		@Override
		public void processAPPStatus() {
			System.out.println("APPStatus requested");
		}

		@Override
		public void processSysInfo() {
			System.out.println("SysInfo requested");
		}

		@Override
		public void processMachineID() {
			try {
				Integer id = streamNode.controllerConnection.readObject();
				streamNode.setNodeID(id);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void processNodeInfo() {
			NodeInfo myInfo = NodeInfo.getMyinfo();
			try {
				streamNode.controllerConnection.writeObject(myInfo);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void processEXIT() {
			System.out.println("StreamNode is Exiting...");
			streamNode.exit();
		}
	}

	public class ProfilerCommandProcessorImpl implements
			ProfilerCommandProcessor {

		ProfilerCommandProcessorImpl() {

		}

		@Override
		public void processSTART() {
			createProfiler();
			if (streamNode.profiler.getState() == Thread.State.NEW)
				streamNode.profiler.start();
		}

		/**
		 * Creates a new profiler only if
		 * <ol>
		 * <li>no any profiler has already been created OR
		 * <li>the previously created profiler has been terminated.
		 * </ol>
		 */
		private void createProfiler() {
			if (streamNode.profiler == null) {
				streamNode.profiler = new Profiler(
						new HashSet<StreamNodeProfiler>(),
						streamNode.controllerConnection);
			} else if (streamNode.profiler.getState() == Thread.State.TERMINATED) {
				System.err
						.println("A profiler has already been created and terminated. Creating another new profiler.");
				streamNode.profiler = new Profiler(
						new HashSet<StreamNodeProfiler>(),
						streamNode.controllerConnection);

			} else
				System.err.println("A profiler has already been started.");
		}

		@Override
		public void processSTOP() {
			streamNode.profiler.stopProfiling();
		}

		@Override
		public void processPAUSE() {
			streamNode.profiler.pauseProfiling();
		}

		@Override
		public void processRESUME() {
			streamNode.profiler.resumeProfiling();
		}
	}
}
