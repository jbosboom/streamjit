package edu.mit.streamjit.tuner;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;

import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.compiler2.Compiler2BlobFactory;
import edu.mit.streamjit.impl.distributed.DistributedBlobFactory;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.BenchmarkProvider;
import edu.mit.streamjit.test.apps.fmradio.FMRadio;

/**
 * ConfigGenerator generates {@link Configuration} of an application and stores
 * it into a database. Later, Opentuner can read this configuration for tuning.
 * In this way, Opentuner can start and stop the StreamJit app for each tuning
 * try so that Opentuner can tune JVM parameters such as heapsize,
 * inlinethreshold, GCpausetime, etc as well.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Sep 10, 2013
 */

public class ConfigGenerator {

	/**
	 * Generates configuration for the passed provider.
	 * 
	 * @param provider
	 *            Only first benchmark is used to generate configuration. i.e.,
	 *            only first benchmark will be tuned.
	 * @param factory
	 */
	public void generate(BenchmarkProvider provider, BlobFactory factory) {
		checkNotNull(provider);

		SqliteAdapter sqlite;
		try {
			sqlite = new SqliteAdapter();
		} catch (ClassNotFoundException e) {
			System.err
					.println("Sqlite3 database not found...couldn't update the database with the configutaion.");
			e.printStackTrace();
			return;
		}

		String dbPath = "streamjit.db";
		sqlite.connectDB(dbPath);
		sqlite.createTable(
				"apps",
				"name string PRIMARY KEY NOT NULL, configuration string, location string, classname string");

		sqlite.createTable("FinalResult",
				"Name string, Config string, Round int, State string, Exectime real");

		String jarFilePath = provider.getClass().getProtectionDomain()
				.getCodeSource().getLocation().getPath();

		String className = provider.getClass().getName();

		Benchmark app = provider.iterator().next();
		// TODO: BlobFactory.getDefaultConfiguration() asks for workers. But
		// from outside workers are not available. need to do something
		// else.

		ConnectWorkersVisitor cwv = new ConnectWorkersVisitor();
		OneToOneElement<?, ?> stream = app.instantiate();
		stream.visit(cwv);
		ImmutableSet<Worker<?, ?>> workers = Workers.getAllWorkersInGraph(cwv
				.getSource());
		Configuration cfg = factory.getDefaultConfiguration(workers);

		String name = app.toString();
		String confString = cfg.toJson();

		try {
			sqlite.executeUpdate(String.format(
					"DELETE FROM apps WHERE name='%s'", name));
		} catch (Exception ex) {
			// Entry not found.
		}

		sqlite.executeUpdate(String.format(
				"insert into apps values('%s', '%s', '%s', '%s')", name,
				confString, jarFilePath, className));

		String tunerPath = String.format(
				"lib%sopentuner%sstreamjit%stuner2.py", File.separator,
				File.separator, File.separator);

		// new ProcessBuilder("xterm", "-e", "python", tunerPath).start();
	}

	/**
	 * @param args
	 *            [0] - NoofMachines that will be connected in distributed case.
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static void main(String[] args) throws InterruptedException,
			IOException {
		int noOfmachines = 1;

		if (args.length > 0) {
			noOfmachines = Integer.parseInt(args[0]);
			if (noOfmachines < 1)
				throw new IllegalArgumentException(String.format(
						"noOfMachines should be 1 or greater. %d passed.",
						noOfmachines));
		}

		BlobFactory bf;
		if (noOfmachines == 1)
			bf = new Compiler2BlobFactory();
		else
			bf = new DistributedBlobFactory(noOfmachines);

		// BenchmarkProvider provider = new ChannelVocoder7();
		BenchmarkProvider provider = new FMRadio.FMRadioBenchmarkProvider();
		// BenchmarkProvider provider = new BitonicSort();
		// BenchmarkProvider provider = new FileInputSanity();
		// BenchmarkProvider provider = new SplitjoinOrderSanity();
		// BenchmarkProvider provider = new HelperFunctionSanity();

		ConfigGenerator cfgGen = new ConfigGenerator();
		cfgGen.generate(provider, bf);
	}
}