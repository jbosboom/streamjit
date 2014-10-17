/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;

import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageVisitor;
import edu.mit.streamjit.impl.distributed.common.Command;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString;
import edu.mit.streamjit.impl.distributed.common.MiscCtrlElements;
import edu.mit.streamjit.impl.distributed.common.NodeInfo;
import edu.mit.streamjit.impl.distributed.common.MiscCtrlElements.MiscCtrlElementProcessor;
import edu.mit.streamjit.impl.distributed.common.MiscCtrlElements.NewConInfo;
import edu.mit.streamjit.impl.distributed.common.Request;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.CTRLRDrainProcessor;
import edu.mit.streamjit.impl.distributed.common.Command.CommandProcessor;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString.ConfigurationStringProcessor;
import edu.mit.streamjit.impl.distributed.common.Request.RequestProcessor;

/**
 * @author Sumanan sumanan@mit.edu
 * @since May 20, 2013
 */
public class CTRLRMessageVisitorImpl implements CTRLRMessageVisitor {

	private final StreamNode streamNode;
	private final RequestProcessor rp;
	private final ConfigurationStringProcessor jp;
	private final MiscCtrlElementProcessor miscProcessor;

	public CTRLRMessageVisitorImpl(StreamNode streamNode) {
		this.streamNode = streamNode;
		this.rp = new RequestProcessorImpl();
		this.jp = new CfgStringProcessorImpl(streamNode);
		this.miscProcessor = new MiscCtrlElementProcessorImpl();
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

	public class MiscCtrlElementProcessorImpl
			implements
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
}
