package edu.mit.streamjit.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import edu.mit.streamjit.impl.common.Configuration;

/**
 * {@link ConfigurationUtils} contains common utility methods those deal with
 * {@link Configuration}.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 10, 2013
 *
 */
public class ConfigurationUtils {

	/**
	 * Reads configuration from ./appName/configurations/namePrefix_appName.cfg
	 * and returns it.
	 * 
	 * @param appName
	 *            name of the streamJit app.
	 * 
	 * @param namePrefix
	 *            prefix to add to the cfg file name.
	 * 
	 * @return {@link Configuration} object if valid file exists. Otherwise
	 *         returns null.
	 */
	public static Configuration readConfiguration(String appName,
			String namePrefix) {
		checkNotNull(appName);
		namePrefix = namePrefix == null ? "" : namePrefix;
		String cfgFilePath = String.format("%s%sconfigurations%s%s_%s.cfg",
				appName, File.separator, File.separator, namePrefix, appName);
		return readConfiguration(cfgFilePath);
	}

	/**
	 * @param cfgFilePath
	 *            path of the configuration file that need to be read.
	 * @return {@link Configuration} object if valid file exists. Otherwise
	 *         returns null.
	 */
	public static Configuration readConfiguration(String cfgFilePath) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(
					cfgFilePath));
			String json = reader.readLine();
			reader.close();
			return Configuration.fromJson(json);
		} catch (IOException ex) {
			System.err.println(String
					.format("File reader error. No %s configuration file.",
							cfgFilePath));
		} catch (Exception ex) {
			System.err.println(String.format(
					"File %s is not a configuration file.", cfgFilePath));
		}
		return null;
	}

	/**
	 * Saves the configuration into
	 * ./appName/configurations/namePrefix_appName.cfg. output _.cfg file will
	 * be named as namePrefix_appName.cfg.
	 * 
	 * @param config
	 *            {@link Configuration} that need to be saved.
	 * @param namePrefix
	 *            prefix to add to the out put file name.
	 * @param appName
	 *            name of the streamJit app. output _.cfg file will be named as
	 *            namePrefix_appName.cfg.
	 */
	public static void saveConfg(Configuration config, String namePrefix,
			String appName) {
		String json = config.toJson();
		saveConfg(json, namePrefix, appName);
	}

	/**
	 * Saves the configuration into
	 * ./appName/configurations/namePrefix_appName.cfg. output _.cfg file will
	 * be named as namePrefix_appName.cfg.
	 * 
	 * @param configJson
	 *            Json representation of the {@link Configuration} that need to
	 *            be saved.
	 * @param namePrefix
	 *            prefix to add to the out put file name.
	 * @param appName
	 *            name of the streamJit app. output _.cfg file will be named as
	 *            namePrefix_appName.cfg.
	 */
	public static void saveConfg(String configJson, String namePrefix,
			String appName) {
		try {

			File dir = new File(String.format("%s%sconfigurations", appName,
					File.separator));
			if (!dir.exists())
				if (!dir.mkdirs()) {
					System.err.println("Make directory failed");
					return;
				}

			File file = new File(dir, String.format("%s_%s.cfg", namePrefix,
					appName));
			FileWriter writer = new FileWriter(file, false);
			writer.write(configJson);
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Adds @param prefix as an extra data to the @param config. Returned
	 * configuration object will contain an extra data named "configPrefix".
	 * 
	 * @param config
	 *            {@link Configuration} object in which the configuration prefix
	 *            need to be added.
	 * @param prefix
	 *            prefix that need to be added to the configuration.
	 * @return Same @param config with configPrefix as an extra data.
	 */
	public static Configuration addConfigPrefix(Configuration config,
			String prefix) {
		if (config == null)
			return config;
		Configuration.Builder builder = Configuration.builder(config);
		builder.putExtraData("configPrefix", prefix);
		return builder.build();
	}

	/**
	 * Gets configuration's prefix name from the configuration and returns.
	 * 
	 * @param config
	 * @return prefix name of the configuration if exists. <code>null</code>
	 *         otherwise.
	 */
	public static String getConfigPrefix(Configuration config) {
		if (config == null)
			return null;
		String prefix = (String) config.getExtraData("configPrefix");
		return prefix == null ? "" : prefix;
	}
}
