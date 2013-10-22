package edu.mit.streamjit.impl.distributed.node;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.Configuration.PartitionParameter;
import edu.mit.streamjit.impl.common.Configuration.PartitionParameter.BlobSpecifier;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.compiler.CompilerBlobFactory;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString.ConfigurationStringProcessor;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.Error;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.NodeInfo;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionProvider;
import edu.mit.streamjit.impl.interp.Interpreter;
import edu.mit.streamjit.util.json.Jsonifiers;

/**
 * {@link ConfigurationStringProcessor} at {@link StreamNode} side.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
public class CfgStringProcessorImpl implements ConfigurationStringProcessor {

	private StreamNode streamNode;

	private Configuration staticConfig = null;

	private TCPConnectionProvider conProvider;

	public CfgStringProcessorImpl(StreamNode streamNode) {
		this.streamNode = streamNode;
	}

	@Override
	public void process(String json, ConfigType type, DrainData drainData) {
		if (type == ConfigType.STATIC) {
			if (this.staticConfig == null) {
				this.staticConfig = Jsonifiers.fromJson(json,
						Configuration.class);

				Map<Integer, NodeInfo> nodeInfoMap = (Map<Integer, NodeInfo>) staticConfig
						.getExtraData(GlobalConstants.NODE_INFO_MAP);

				this.conProvider = new TCPConnectionProvider(
						streamNode.getNodeID(), nodeInfoMap);
			} else
				System.err
						.println("New static configuration received...But Ignored...");
		} else {
			Configuration cfg = Jsonifiers.fromJson(json, Configuration.class);
			ImmutableSet<Blob> blobSet = getBlobs(cfg, staticConfig, drainData);
			if (blobSet != null) {

				Map<Token, TCPConnectionInfo> conInfoMap = (Map<Token, TCPConnectionInfo>) cfg
						.getExtraData(GlobalConstants.CONINFOMAP);

				streamNode.setBlobsManager(new BlobsManagerImpl(blobSet,
						conInfoMap, streamNode, conProvider));
			} else
				System.out.println("Couldn't get the blobset....");
		}
	}

	private ImmutableSet<Blob> getBlobs(Configuration dyncfg,
			Configuration stccfg, DrainData drainData) {

		PartitionParameter partParam = dyncfg.getParameter(
				GlobalConstants.PARTITION, PartitionParameter.class);
		if (partParam == null)
			throw new IllegalArgumentException(
					"Partition parameter is not available in the received configuraion");

		String topLevelWorkerName = (String) stccfg
				.getExtraData(GlobalConstants.TOPLEVEL_WORKER_NAME);
		String jarFilePath = (String) stccfg
				.getExtraData(GlobalConstants.JARFILE_PATH);

		OneToOneElement<?, ?> streamGraph = getStreamGraph(jarFilePath,
				topLevelWorkerName);
		if (streamGraph != null) {
			ConnectWorkersVisitor primitiveConnector = new ConnectWorkersVisitor();
			streamGraph.visit(primitiveConnector);
			Worker<?, ?> source = primitiveConnector.getSource();

			List<BlobSpecifier> blobList = partParam
					.getBlobsOnMachine(streamNode.getNodeID());

			ImmutableSet.Builder<Blob> blobSet = ImmutableSet.builder();

			if (blobList == null)
				return blobSet.build();

			BlobFactory bf;
			Configuration blobConfigs = dyncfg
					.getSubconfiguration("blobConfigs");
			blobConfigs = null;
			if (blobConfigs == null) {
				blobConfigs = staticConfig;
				bf = new Interpreter.InterpreterBlobFactory();
			} else {
				IntParameter mul = blobConfigs.getParameter(
						String.format("multiplier%d", streamNode.getNodeID()),
						IntParameter.class);
				Configuration.Builder builder = Configuration
						.builder(blobConfigs);
				builder.removeParameter(mul.getName());
				builder.addParameter(new IntParameter("multiplier", mul
						.getRange(), mul.getValue()));
				blobConfigs = builder.build();
				bf = new CompilerBlobFactory();
			}

			for (BlobSpecifier bs : blobList) {
				Set<Integer> workIdentifiers = bs.getWorkerIdentifiers();
				System.out
						.println(String
								.format("DEBUG: %s - Following workers have been assigned to me. %s",
										Thread.currentThread().getName(),
										workIdentifiers.toString()));
				ImmutableSet<Worker<?, ?>> workerset = bs.getWorkers(source);

				Blob b = bf.makeBlob(workerset, blobConfigs, 1, drainData);
				blobSet.add(b);
			}
			return blobSet.build();
		} else
			return null;
	}

	/**
	 * Gets a Stream Graph from a jar file.
	 * 
	 * @param jarFilePath
	 * @param topStreamClassName
	 * @return : StreamGraph
	 */
	private OneToOneElement<?, ?> getStreamGraph(String jarFilePath,
			String topStreamClassName) {
		checkNotNull(jarFilePath);
		checkNotNull(topStreamClassName);

		File jarFile = new java.io.File(jarFilePath);
		if (!jarFile.exists()) {
			System.out.println("Jar file not found....");
			try {
				streamNode.controllerConnection
						.writeObject(Error.FILE_NOT_FOUND);
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}

		// In some benchmarks, top level stream class is written as an static
		// inner class. So in that case, we have to find the outer
		// class first. Java's Class.getName() returns "OutterClass$InnerClass"
		// format. So if $ exists in the method argument
		// topStreamClassName then the actual top level stream class is lies
		// inside another class.
		String outterClassName = null;
		if (topStreamClassName.contains("$")) {
			int pos = topStreamClassName.indexOf("$");
			outterClassName = (String) topStreamClassName.subSequence(0, pos);
			topStreamClassName = topStreamClassName.substring(pos + 1);
		}

		URL url;
		try {
			url = jarFile.toURI().toURL();
			URL[] urls = new URL[]{url};

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
				streamNode.controllerConnection
						.writeObject(Error.WORKER_NOT_FOUND);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			// System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Couldn't find the toplevel worker.");

			// TODO: Try catch inside a catch block. Good practice???
			try {
				streamNode.controllerConnection
						.writeObject(Error.WORKER_NOT_FOUND);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		return null;
	}

	private static Class<?> getInngerClass(Class<?> OutterClass,
			String InnterClassName) throws ClassNotFoundException {
		Class<?>[] kl = OutterClass.getClasses();
		for (Class<?> k : kl) {
			if (InnterClassName.equals(k.getSimpleName())) {
				return k;
			}
		}
		throw new ClassNotFoundException(
				String.format(
						"Innter class %s is not found in the outter class %s. Check the accessibility/visibility of the inner class",
						InnterClassName, OutterClass.getName()));
	}
}
