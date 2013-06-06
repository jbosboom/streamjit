/**
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
package edu.mit.streamjit.impl.distributed.runtime.slave;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import edu.mit.streamjit.impl.distributed.runtime.api.NodeInfo;
import edu.mit.streamjit.impl.distributed.runtime.api.RequestProcessor;

public class SlaveRequestProcessor implements RequestProcessor {

	Slave slave;

	SlaveRequestProcessor(Slave slave) {
		this.slave = slave;
	}

	@Override
	public void processAPPStatus() {
		System.out.println("APPStatus requested");
	}

	@Override
	public void processSysInfo() {
		System.out.println("SysInfo requested");
	}

	@Override
	public void processMaxCores() {
		try {
			slave.masterConnection.writeObject(new Integer(Runtime.getRuntime().availableProcessors()));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void processMachineID() {
		try {
			Integer id = slave.masterConnection.readObject();
			slave.setMachineID(id);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void processNodeInfo() {

		InetAddress localMachine;
		String hostName;
		int availableCores = Runtime.getRuntime().availableProcessors();
		// TODO: Need to verify the ramSize.
		long ramSize = Runtime.getRuntime().maxMemory();
		try {
			localMachine = InetAddress.getLocalHost();
			hostName = localMachine.getHostName();
			NodeInfo myInfo = new NodeInfo(hostName, localMachine, availableCores, ramSize);
			slave.masterConnection.writeObject(myInfo);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
