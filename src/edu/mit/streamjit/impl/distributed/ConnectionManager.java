package edu.mit.streamjit.impl.distributed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.Builder;
import edu.mit.streamjit.impl.common.Configuration.Parameter;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.distributed.common.AsynchronousTCPConnection.AsyncTCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel;
import edu.mit.streamjit.impl.distributed.common.Connection;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionType;
import edu.mit.streamjit.impl.distributed.common.Connection.GenericConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.node.StreamNode;

/**
 * Generates configuration parameters to tune the {@link Connection}'s
 * communication type such as blocking TCP connection, asynchronous TCP
 * connection, Infiniband, etc.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Jun 23, 2014
 * 
 */
public interface ConnectionManager {

	/**
	 * Generates parameters to tune {@link BoundaryChannel} and add those into
	 * the {@link Configuration.Builder}.
	 * 
	 * @param cfgBuilder
	 * @param workers
	 */
	public void addChannelParameters(Configuration.Builder cfgBuilder,
			Set<Worker<?, ?>> workers);

	/**
	 * Generates parameters to tune {@link BoundaryChannel} and return those as
	 * a new {@link Configuration}.
	 * 
	 * @param workers
	 * @return
	 */
	public Configuration getDefaultConfiguration(Set<Worker<?, ?>> workers);

	/**
	 * Decides {@link Connection} type for each inter-blob connection based on
	 * the {@link cfg}.
	 * 
	 * @param cfg
	 * @param partitionsMachineMap
	 * @param source
	 * @param sink
	 * @return
	 */
	public Map<Token, ConnectionInfo> conInfoMap(Configuration cfg,
			Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap,
			Worker<?, ?> source, Worker<?, ?> sink);

	/**
	 * Sometimes an assigned TCP ports may not available to make new connection
	 * at {@link StreamNode}s side. In this case a new {@link ConnectionInfo}
	 * must be created to replace already created {@link ConnectionInfo}.
	 * 
	 * @param conInfo
	 *            : Problematic {@link ConnectionInfo}.
	 * @return : New {@link ConnectionInfo} to replace problematic
	 *         {@link ConnectionInfo}.
	 */
	public ConnectionInfo replaceConInfo(ConnectionInfo conInfo);

	public abstract static class AbstractConnectionManager implements
			ConnectionManager {

		private final int controllerNodeID;

		protected Set<ConnectionInfo> currentConInfos;

		protected int startPortNo = 24896; // Just a random magic number.

		public AbstractConnectionManager(int controllerNodeID) {
			this.controllerNodeID = controllerNodeID;
			this.currentConInfos = new HashSet<>();
		}

		public Map<Token, ConnectionInfo> conInfoMap(Configuration cfg,
				Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap,
				Worker<?, ?> source, Worker<?, ?> sink) {

			assert partitionsMachineMap != null : "partitionsMachineMap is null";

			Set<ConnectionInfo> usedConInfos = new HashSet<>();
			Map<Token, ConnectionInfo> conInfoMap = new HashMap<>();

			for (Integer machineID : partitionsMachineMap.keySet()) {
				List<Set<Worker<?, ?>>> blobList = partitionsMachineMap
						.get(machineID);
				Set<Worker<?, ?>> allWorkers = new HashSet<>(); // Contains all
																// workers those
																// are
																// assigned to
																// the
																// current
																// machineID
																// machine.
				for (Set<Worker<?, ?>> blobWorkers : blobList) {
					allWorkers.addAll(blobWorkers);
				}

				for (Worker<?, ?> w : allWorkers) {
					for (Worker<?, ?> succ : Workers.getSuccessors(w)) {
						if (allWorkers.contains(succ))
							continue;
						int dstMachineID = getAssignedMachine(succ,
								partitionsMachineMap);
						Token t = new Token(w, succ);
						addtoconInfoMap(machineID, dstMachineID, t,
								usedConInfos, conInfoMap, cfg);
					}
				}
			}

			Token headToken = Token.createOverallInputToken(source);
			int dstMachineID = getAssignedMachine(source, partitionsMachineMap);
			addtoconInfoMap(controllerNodeID, dstMachineID, headToken,
					usedConInfos, conInfoMap, cfg);

			Token tailToken = Token.createOverallOutputToken(sink);
			int srcMahineID = getAssignedMachine(sink, partitionsMachineMap);
			addtoconInfoMap(srcMahineID, controllerNodeID, tailToken,
					usedConInfos, conInfoMap, cfg);

			return conInfoMap;
		}

		/**
		 * @param worker
		 * @return the machineID where on which the passed worker is assigned.
		 */
		private int getAssignedMachine(Worker<?, ?> worker,
				Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap) {
			for (Integer machineID : partitionsMachineMap.keySet()) {
				for (Set<Worker<?, ?>> workers : partitionsMachineMap
						.get(machineID)) {
					if (workers.contains(worker))
						return machineID;
				}
			}

			throw new IllegalArgumentException(String.format(
					"%s is not assigned to anyof the machines", worker));
		}

		protected abstract void addtoconInfoMap(int srcID, int dstID, Token t,
				Set<ConnectionInfo> usedConInfos,
				Map<Token, ConnectionInfo> conInfoMap, Configuration cfg);

		protected List<ConnectionInfo> getTcpConInfo(ConnectionInfo conInfo) {
			List<ConnectionInfo> conList = new ArrayList<>();
			for (ConnectionInfo tcpconInfo : currentConInfos) {
				if (conInfo.equals(tcpconInfo))
					conList.add(tcpconInfo);
			}
			return conList;
		}

		protected String getParamName(Token t) {
			return String.format("ConnectionType-%d:%d",
					t.getUpstreamIdentifier(), t.getDownstreamIdentifier());
		}

		public ConnectionInfo replaceConInfo(ConnectionInfo conInfo) {
			if (currentConInfos.contains(conInfo))
				currentConInfos.remove(conInfo);
			ConnectionInfo newConinfo;
			if (conInfo.getSrcID() == 0)
				newConinfo = new TCPConnectionInfo(conInfo.getSrcID(),
						conInfo.getDstID(), startPortNo++);
			else
				newConinfo = new AsyncTCPConnectionInfo(conInfo.getSrcID(),
						conInfo.getDstID(), startPortNo++);
			currentConInfos.add(newConinfo);

			return newConinfo;
		}
	}

	public static abstract class NoParams extends AbstractConnectionManager {

		public NoParams(int controllerNodeID) {
			super(controllerNodeID);
		}

		@Override
		public void addChannelParameters(Builder cfgBuilder,
				Set<Worker<?, ?>> workers) {
			return;
		}

		@Override
		public Configuration getDefaultConfiguration(Set<Worker<?, ?>> workers) {
			return Configuration.builder().build();
		}

		protected void addtoconInfoMap(int srcID, int dstID, Token t,
				Set<ConnectionInfo> usedConInfos,
				Map<Token, ConnectionInfo> conInfoMap, Configuration cfg) {

			ConnectionInfo conInfo = new GenericConnectionInfo(srcID, dstID);

			List<ConnectionInfo> conSet = getTcpConInfo(conInfo);
			ConnectionInfo tcpConInfo = null;

			for (ConnectionInfo con : conSet) {
				if (!usedConInfos.contains(con)) {
					tcpConInfo = con;
					break;
				}
			}

			if (tcpConInfo == null) {
				tcpConInfo = makeConnectionInfo(srcID, dstID);
				this.currentConInfos.add(tcpConInfo);
			}

			conInfoMap.put(t, tcpConInfo);
			usedConInfos.add(tcpConInfo);
		}

		protected abstract ConnectionInfo makeConnectionInfo(int srcID,
				int dstID);
	}

	public static class BlockingTCPNoParams extends NoParams {

		public BlockingTCPNoParams(int controllerNodeID) {
			super(controllerNodeID);
		}

		@Override
		protected ConnectionInfo makeConnectionInfo(int srcID, int dstID) {
			return new TCPConnectionInfo(srcID, dstID, startPortNo++);
		}
	}

	public static class AsyncTCPNoParams extends NoParams {

		public AsyncTCPNoParams(int controllerNodeID) {
			super(controllerNodeID);
		}

		@Override
		protected ConnectionInfo makeConnectionInfo(int srcID, int dstID) {
			return new AsyncTCPConnectionInfo(srcID, dstID, startPortNo++);
		}
	}

	public static class AllConnectionParams extends AbstractConnectionManager {
		public AllConnectionParams(int controllerNodeID) {
			super(controllerNodeID);
		}

		@Override
		public void addChannelParameters(Builder cfgBuilder,
				Set<Worker<?, ?>> workers) {
			for (Worker<?, ?> w : workers) {
				for (Worker<?, ?> succ : Workers.getSuccessors(w)) {
					Token t = new Token(w, succ);
					Parameter p = new Configuration.SwitchParameter<ConnectionType>(
							getParamName(t), ConnectionType.class,
							ConnectionType.BTCP, Arrays.asList(ConnectionType
									.values()));
					cfgBuilder.addParameter(p);
				}
			}

			// Add Parameter for global input channel.
			Set<Worker<?, ?>> heads = Workers.getTopmostWorkers(workers);
			assert heads.size() == 1 : "Multiple first workers";
			for (Worker<?, ?> firstWorker : heads) {
				Token t = Token.createOverallInputToken(firstWorker);
				Parameter p = new Configuration.SwitchParameter<ConnectionType>(
						getParamName(t), ConnectionType.class,
						ConnectionType.BTCP, Arrays.asList(ConnectionType
								.values()));
				cfgBuilder.addParameter(p);
			}

			// Add Parameter for global output channel.
			Set<Worker<?, ?>> tail = Workers.getBottommostWorkers(workers);
			assert tail.size() == 1 : "Multiple first workers";
			for (Worker<?, ?> lastWorker : tail) {
				Token t = Token.createOverallOutputToken(lastWorker);
				Parameter p = new Configuration.SwitchParameter<ConnectionType>(
						getParamName(t), ConnectionType.class,
						ConnectionType.BTCP, Arrays.asList(ConnectionType
								.values()));
				cfgBuilder.addParameter(p);
			}
		}

		@Override
		public Configuration getDefaultConfiguration(Set<Worker<?, ?>> workers) {
			Configuration.Builder cfgBuilder = Configuration.builder();
			addChannelParameters(cfgBuilder, workers);
			return cfgBuilder.build();
		}

		protected void addtoconInfoMap(int srcID, int dstID, Token t,
				Set<ConnectionInfo> usedConInfos,
				Map<Token, ConnectionInfo> conInfoMap, Configuration cfg) {

			ConnectionInfo conInfo = new GenericConnectionInfo(srcID, dstID);

			List<ConnectionInfo> conSet = getTcpConInfo(conInfo);
			ConnectionInfo tcpConInfo = null;

			for (ConnectionInfo con : conSet) {
				if (!usedConInfos.contains(con)) {
					tcpConInfo = con;
					break;
				}
			}

			if (tcpConInfo == null) {
				tcpConInfo = makeConnectionInfo(srcID, dstID, t, cfg);
				this.currentConInfos.add(tcpConInfo);
			}

			conInfoMap.put(t, tcpConInfo);
			usedConInfos.add(tcpConInfo);
		}

		private ConnectionInfo makeConnectionInfo(int srcID, int dstID,
				Token t, Configuration cfg) {
			SwitchParameter<ConnectionType> p = cfg.getParameter(
					getParamName(t), SwitchParameter.class,
					ConnectionType.class);

			if (p == null)
				throw new IllegalStateException(String.format(
						"No tuning parameter for connection %s", t));

			ConnectionInfo conInfo;
			switch (p.getValue()) {
				case BTCP :
					conInfo = new TCPConnectionInfo(srcID, dstID, startPortNo++);
					break;
				case ATCP :
					conInfo = new AsyncTCPConnectionInfo(srcID, dstID,
							startPortNo++);
					break;
				default :
					throw new IllegalStateException(String.format(
							"Unsupported connection type - %s", p.getValue()));
			}
			return conInfo;
		}
	}
}
