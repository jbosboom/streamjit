package edu.mit.streamjit.util;

import java.io.BufferedReader;
import java.io.FileReader;
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
}
