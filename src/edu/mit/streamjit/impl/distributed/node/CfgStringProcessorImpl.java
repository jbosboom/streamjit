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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
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
import edu.mit.streamjit.impl.common.Configuration.PartitionParameter;
import edu.mit.streamjit.impl.common.Configuration.PartitionParameter.BlobSpecifier;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.compiler2.Compiler2BlobFactory;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString.ConfigurationStringProcessor;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.Error;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
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

				Map<Integer, InetAddress> iNetAddressMap = (Map<Integer, InetAddress>) staticConfig
						.getExtraData(GlobalConstants.INETADDRESS_MAP);

				this.conProvider = new TCPConnectionProvider(
						streamNode.getNodeID(), iNetAddressMap);
			} else
				System.err
						.println("New static configuration received...But Ignored...");
		} else {
			System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
			System.out.println("New Configuration.....");
			Configuration cfg = Jsonifiers.fromJson(json, Configuration.class);
			ImmutableSet<Blob> blobSet = getBlobs(cfg, staticConfig, drainData);
			if (blobSet != null) {
				try {
					streamNode.controllerConnection
							.writeObject(AppStatus.COMPILED);
				} catch (IOException e) {
					e.printStackTrace();
				}

				Map<Token, TCPConnectionInfo> conInfoMap = (Map<Token, TCPConnectionInfo>) cfg
						.getExtraData(GlobalConstants.CONINFOMAP);

				streamNode.setBlobsManager(new BlobsManagerImpl(blobSet,
						conInfoMap, streamNode, conProvider));
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
			if (blobConfigs == null) {
				blobConfigs = staticConfig;
				bf = new Interpreter.InterpreterBlobFactory();
			} else {
				bf = new Compiler2BlobFactory();
			}

			for (BlobSpecifier bs : blobList) {
				Set<Integer> workIdentifiers = bs.getWorkerIdentifiers();
				// DEBUG
				System.out.println(String.format(
						"A new blob with workers %s has been created.",
						workIdentifiers.toString()));
				ImmutableSet<Worker<?, ?>> workerset = bs.getWorkers(source);

				try {
					Blob b = bf.makeBlob(workerset, blobConfigs, 1, drainData);
					blobSet.add(b);
				} catch (Exception ex) {
					return null;
				}
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
}
