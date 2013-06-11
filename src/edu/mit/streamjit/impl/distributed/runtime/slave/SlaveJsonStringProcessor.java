package edu.mit.streamjit.impl.distributed.runtime.slave;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.PartitionParameter;
import edu.mit.streamjit.impl.common.Configuration.PartitionParameter.BlobSpecifier;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.common.MessageConstraint;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.distributed.runtime.api.Error;
import edu.mit.streamjit.impl.distributed.runtime.api.JsonStringProcessor;
import edu.mit.streamjit.impl.distributed.runtime.api.NodeInfo;
import edu.mit.streamjit.impl.distributed.runtime.common.GlobalConstants;
import edu.mit.streamjit.impl.interp.ArrayChannel;
import edu.mit.streamjit.impl.interp.Channel;
import edu.mit.streamjit.impl.interp.SynchronizedChannel;
import edu.mit.streamjit.util.json.Jsonifiers;

/**
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
public class SlaveJsonStringProcessor implements JsonStringProcessor {

	Slave slave;

	public SlaveJsonStringProcessor(Slave slave) {
		this.slave = slave;
	}

	@Override
	public void process(String json) {

		Configuration cfg = Jsonifiers.fromJson(json, Configuration.class);
		ImmutableSet<Blob> blobSet = getBlobs(cfg);
		if (blobSet != null) {
			Map<Token, Map.Entry<Integer, Integer>> tokenMachineMap = (Map<Token, Map.Entry<Integer, Integer>>) cfg
					.getExtraData(GlobalConstants.TOKEN_MACHINE_MAP);
			Map<Token, Integer> portIdMap = (Map<Token, Integer>) cfg.getExtraData(GlobalConstants.PORTID_MAP);

			Map<Integer, NodeInfo> nodeInfoMap = (Map<Integer, NodeInfo>) cfg.getExtraData(GlobalConstants.NODE_INFO_MAP);

			slave.setBlobsManager(new BlobsManagerImpl(blobSet, tokenMachineMap, portIdMap, nodeInfoMap));
		} else
			System.out.println("Couldn't get the blobset....");
	}

	private ImmutableSet<Blob> getBlobs(Configuration cfg) {

		PartitionParameter partParam = cfg.getParameter(GlobalConstants.PARTITION, PartitionParameter.class);
		if (partParam == null)
			throw new IllegalArgumentException("Partition parameter is not available in the received configuraion");

		String outterClass = (String) cfg.getExtraData(GlobalConstants.OUTTER_CLASS_NAME);
		String topLevelWorkerName = (String) cfg.getExtraData(GlobalConstants.TOPLEVEL_WORKER_NAME);
		String jarFilePath = (String) cfg.getExtraData(GlobalConstants.JARFILE_PATH);

		OneToOneElement<?, ?> streamGraph = getStreamGraph(jarFilePath, topLevelWorkerName);
		if (streamGraph != null) {
			ConnectWorkersVisitor primitiveConnector = new ConnectWorkersVisitor();
			streamGraph.visit(primitiveConnector);
			Worker<?, ?> source = (Worker<?, ?>) primitiveConnector.getSource();
			Worker<?, ?> sink = (Worker<?, ?>) primitiveConnector.getSink();

			// TODO: Need to ensure the type arguments of this channels.
			Channel head = new SynchronizedChannel<>(new ArrayChannel<>());
			Channel tail = new SynchronizedChannel<>(new ArrayChannel<>());

			Workers.getInputChannels(source).add(head);
			Workers.getOutputChannels(sink).add(tail);

			List<BlobSpecifier> blobList = partParam.getBlobsOnMachine(slave.getMachineID());

			ImmutableSet.Builder<Blob> blobSet = ImmutableSet.builder();

			for (BlobSpecifier bs : blobList) {
				Set<Integer> workIdentifiers = bs.getWorkerIdentifiers();
				System.out.println(workIdentifiers.toString());
				ImmutableSet<Worker<?, ?>> workerset = bs.getWorkers(source);
				// TODO: Need to partitions the workerset to threads. Lets do the equal partitioning.
				Blob b = new DistributedBlob(partitionEqually(workerset, 1), Collections.<MessageConstraint> emptyList());

				blobSet.add(b);
			}
			return blobSet.build();
		} else
			return null;
	}

	/**
	 * @param jarFilePath
	 * @param outterClassName
	 *            : Some of the benchmarks are written inside the ............TODO: Add a descriptive comment about the purpose of the
	 *            outterclass at all places.
	 * @param topStreamClassName
	 * @return
	 */
	private OneToOneElement<?, ?> getStreamGraph(String jarFilePath, String topStreamClassName) {

		checkNotNull(jarFilePath);
		checkNotNull(topStreamClassName);

		// In some benchmarks, top level stream class is written as an static inner class. So in that case, we have to find the outer
		// class first.
		String outterClassName = null;

		// Java's Class.getName() returns "OutterClass$InnerClass" format. So if $ exists in the topStreamClassName, actual top level
		// stream class is written as a inner class.
		if (topStreamClassName.contains("$")) {
			int pos = topStreamClassName.indexOf("$");
			outterClassName = (String) topStreamClassName.subSequence(0, pos);
			topStreamClassName = topStreamClassName.substring(pos + 1);
		}

		File jarFile = new java.io.File(jarFilePath);
		if (!jarFile.exists()) {
			System.out.println("Jar file not found....");
			try {
				slave.masterConnection.writeObject(Error.FILE_NOT_FOUND);
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}

		URL url;
		try {
			url = jarFile.toURI().toURL();
			URL[] urls = new URL[] { url };

			ClassLoader loader = new URLClassLoader(urls);
			Class<?> topStreamClass;
			if (!Strings.isNullOrEmpty(outterClassName)) {
				Class<?> clazz1 = loader.loadClass(outterClassName);
				topStreamClass = getInngerClass(clazz1, topStreamClassName);
			} else {
				topStreamClass = loader.loadClass(topStreamClassName);
			}
			System.out.println(topStreamClass.getSimpleName());
			return (OneToOneElement<?, ?>) topStreamClass.newInstance();

		} catch (MalformedURLException e) {
			e.printStackTrace();
			System.out.println("Couldn't find the toplevel worker...Exiting");

			// TODO: Try catch inside a catch block. Good practice???
			try {
				slave.masterConnection.writeObject(Error.WORKER_NOT_FOUND);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			// System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Couldn't find the toplevel worker...Exiting");

			// TODO: Try catch inside a catch block. Good practice???
			try {
				slave.masterConnection.writeObject(Error.WORKER_NOT_FOUND);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			// System.exit(0);
		}
		return null;
	}

	private static Class<?> getInngerClass(Class<?> OutterClass, String InnterClassName) throws ClassNotFoundException {
		Class<?>[] kl = OutterClass.getClasses();
		for (Class<?> k : kl) {
			if (InnterClassName.equals(k.getSimpleName())) {
				return k;
			}
		}
		throw new ClassNotFoundException(String.format("Innter class %s is not found in the outter class %s", InnterClassName,
				OutterClass.getName()));
	}

	/**
	 * Just does the round robin assignment. TODO: Need to optimally assign the workers to the threads.
	 * 
	 * @param workerSet
	 * @param noOfPartitions
	 * @return
	 */
	private List<Set<Worker<?, ?>>> partitionEqually(Set<Worker<?, ?>> workerSet, int noOfPartitions) {
		List<Set<Worker<?, ?>>> partList = new ArrayList<Set<Worker<?, ?>>>(noOfPartitions);
		for (int i = 0; i < noOfPartitions; i++) {
			partList.add(new HashSet<Worker<?, ?>>());
		}

		int j = 0;
		for (Worker<?, ?> w : workerSet) {
			partList.get(j++).add(w);
			if (j == partList.size())
				j = 0;
		}
		return partList;
	}
}
