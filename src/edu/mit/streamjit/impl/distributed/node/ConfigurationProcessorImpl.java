package edu.mit.streamjit.impl.distributed.node;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.PartitionParameter;
import edu.mit.streamjit.impl.common.Configuration.PartitionParameter.BlobSpecifier;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString.ConfigurationProcessor;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionProvider;
import edu.mit.streamjit.impl.distributed.common.Error;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.NetworkInfo;
import edu.mit.streamjit.impl.distributed.common.SNTimeInfo.CompilationTime;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.util.json.Jsonifiers;

/**
 * {@link ConfigurationProcessor} at {@link StreamNode} side.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
public class ConfigurationProcessorImpl implements ConfigurationProcessor {

	private StreamNode streamNode;

	private Configuration staticConfig = null;

	private ConnectionProvider conProvider;

	public ConfigurationProcessorImpl(StreamNode streamNode) {
		this.streamNode = streamNode;
	}

	@Override
	public void process(String json, ConfigType type, DrainData drainData) {
		if (type == ConfigType.STATIC) {
			processStaticCfg(json);
		} else {
			processDynamicCfg(json, drainData);
		}
	}

	private void processStaticCfg(String json) {
		if (this.staticConfig == null) {
			this.staticConfig = Jsonifiers.fromJson(json, Configuration.class);

			Map<Integer, InetAddress> iNetAddressMap = (Map<Integer, InetAddress>) staticConfig
					.getExtraData(GlobalConstants.INETADDRESS_MAP);

			NetworkInfo networkInfo = new NetworkInfo(iNetAddressMap);

			this.conProvider = new ConnectionProvider(streamNode.getNodeID(),
					networkInfo);
		} else
			System.err
					.println("New static configuration received...But Ignored...");
	}

	private void processDynamicCfg(String json, DrainData drainData) {
		System.out
				.println("------------------------------------------------------------");
		System.out.println("New Configuration.....");
		streamNode.releaseOldBM();
		Configuration cfg = Jsonifiers.fromJson(json, Configuration.class);
		ImmutableSet<Blob> blobSet = getBlobs(cfg, drainData);
		if (blobSet != null) {
			try {
				streamNode.controllerConnection.writeObject(AppStatus.COMPILED);
			} catch (IOException e) {
				e.printStackTrace();
			}

			Map<Token, ConnectionInfo> conInfoMap = (Map<Token, ConnectionInfo>) cfg
					.getExtraData(GlobalConstants.CONINFOMAP);

			String appName = (String) staticConfig
					.getExtraData(GlobalConstants.TOPLEVEL_WORKER_NAME);
			streamNode.setBlobsManager(new BlobsManagerImpl(blobSet,
					conInfoMap, streamNode, conProvider, appName));
		} else {
			try {
				streamNode.controllerConnection
						.writeObject(AppStatus.COMPILATION_ERROR);
			} catch (IOException e) {
				e.printStackTrace();
			}

			System.out.println("Couldn't get the blobset....");
		}
	}

	private ImmutableSet<Blob> getBlobs(Configuration dyncfg,
			DrainData drainData) {

		PartitionParameter partParam = dyncfg.getParameter(
				GlobalConstants.PARTITION, PartitionParameter.class);
		if (partParam == null)
			throw new IllegalArgumentException(
					"Partition parameter is not available in the received configuraion");

		OneToOneElement<?, ?> streamGraph = getStreamGraph();
		if (streamGraph != null) {
			ConnectWorkersVisitor primitiveConnector = new ConnectWorkersVisitor();
			streamGraph.visit(primitiveConnector);
			Worker<?, ?> source = primitiveConnector.getSource();

			List<BlobSpecifier> blobList = partParam
					.getBlobsOnMachine(streamNode.getNodeID());

			ImmutableSet.Builder<Blob> blobSet = ImmutableSet.builder();

			if (blobList == null)
				return blobSet.build();

			Configuration blobConfigs = dyncfg
					.getSubconfiguration("blobConfigs");
			return blobset(blobSet, blobList, drainData, blobConfigs, source);

		} else
			return null;
	}

	private void sendCompilationTime(Stopwatch sw, Token blobID) {
		sw.stop();
		CompilationTime ct = new CompilationTime(blobID,
				sw.elapsed(TimeUnit.MILLISECONDS));
		try {
			streamNode.controllerConnection.writeObject(ct);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Gets a Stream Graph from a jar file.
	 * 
	 * @return : StreamGraph
	 */
	private OneToOneElement<?, ?> getStreamGraph() {
		String topStreamClassName = (String) staticConfig
				.getExtraData(GlobalConstants.TOPLEVEL_WORKER_NAME);
		String jarFilePath = (String) staticConfig
				.getExtraData(GlobalConstants.JARFILE_PATH);

		checkNotNull(jarFilePath);
		checkNotNull(topStreamClassName);
		jarFilePath = this.getClass().getProtectionDomain().getCodeSource()
				.getLocation().getPath();
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
				streamNode.controllerConnection
						.writeObject(Error.WORKER_NOT_FOUND);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			// System.exit(0);
		} catch (InstantiationException iex) {
			System.err.println("InstantiationException exception.");
			System.err
					.println("Please ensure the top level StreamJit application"
							+ " class is public and have no argument constructor.");
			iex.printStackTrace();
		} catch (Exception e) {
			System.out.println("Couldn't find the toplevel worker.");
			e.printStackTrace();

			// TODO: Try catch inside a catch block. Good practice???
			try {
				streamNode.controllerConnection
						.writeObject(Error.WORKER_NOT_FOUND);
			} catch (IOException e1) {
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

	private ImmutableSet<Blob> blobset(ImmutableSet.Builder<Blob> blobSet,
			List<BlobSpecifier> blobList, DrainData drainData,
			Configuration blobConfigs, Worker<?, ?> source) {
		for (BlobSpecifier bs : blobList) {
			Set<Integer> workIdentifiers = bs.getWorkerIdentifiers();
			ImmutableSet<Worker<?, ?>> workerset = bs.getWorkers(source);
			try {
				BlobFactory bf = bs.getBlobFactory();
				int maxCores = bs.getCores();
				Stopwatch sw = Stopwatch.createStarted();
				DrainData dd = drainData == null ? null : drainData
						.subset(workIdentifiers);
				Blob b = bf.makeBlob(workerset, blobConfigs, maxCores, dd);
				sendCompilationTime(sw, Utils.getblobID(workerset));
				blobSet.add(b);
			} catch (Exception ex) {
				ex.printStackTrace();
				return null;
			} catch (OutOfMemoryError er) {
				Utils.printOutOfMemory();
				return null;
			}
			// DEBUG MSG
			if (!GlobalConstants.singleNodeOnline)
				System.out.println(String.format(
						"A new blob with workers %s has been created.",
						workIdentifiers.toString()));
		}
		System.out.println("All blobs have been created");
		return blobSet.build();
	}

	/**
	 * Compiles the blobs in parallel.
	 */
	private ImmutableSet<Blob> blobset1(ImmutableSet.Builder<Blob> blobSet,
			List<BlobSpecifier> blobList, DrainData drainData,
			Configuration blobConfigs, Worker<?, ?> source) {
		Set<Future<Blob>> futures = new HashSet<>();
		ExecutorService executerSevce = Executors.newFixedThreadPool(blobList
				.size());

		for (BlobSpecifier bs : blobList) {
			MakeBlob mb = new MakeBlob(bs, drainData, blobConfigs, source);
			Future<Blob> f = executerSevce.submit(mb);
			futures.add(f);
		}

		executerSevce.shutdown();

		while (!executerSevce.isTerminated()) {
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		for (Future<Blob> f : futures) {
			Blob b;
			try {
				b = f.get();
				if (b == null)
					return null;
				blobSet.add(b);
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}

		System.out.println("All blobs have been created");
		return blobSet.build();
	}

	private class MakeBlob implements Callable<Blob> {
		private final BlobSpecifier bs;
		private final DrainData drainData;
		private final Configuration blobConfigs;
		private final Worker<?, ?> source;

		private MakeBlob(BlobSpecifier bs, DrainData drainData,
				Configuration blobConfigs, Worker<?, ?> source) {
			this.bs = bs;
			this.drainData = drainData;
			this.blobConfigs = blobConfigs;
			this.source = source;
		}

		@Override
		public Blob call() throws Exception {
			Blob b = null;
			Set<Integer> workIdentifiers = bs.getWorkerIdentifiers();
			ImmutableSet<Worker<?, ?>> workerset = bs.getWorkers(source);
			try {
				BlobFactory bf = bs.getBlobFactory();
				int maxCores = bs.getCores();
				Stopwatch sw = Stopwatch.createStarted();
				DrainData dd = drainData == null ? null : drainData
						.subset(workIdentifiers);
				b = bf.makeBlob(workerset, blobConfigs, maxCores, dd);
				sendCompilationTime(sw, Utils.getblobID(workerset));
			} catch (Exception ex) {
				ex.printStackTrace();
			} catch (OutOfMemoryError er) {
				Utils.printOutOfMemory();
			}
			// DEBUG MSG
			if (!GlobalConstants.singleNodeOnline && b != null)
				System.out.println(String.format(
						"A new blob with workers %s has been created.",
						workIdentifiers.toString()));
			return b;
		}
	}
}
