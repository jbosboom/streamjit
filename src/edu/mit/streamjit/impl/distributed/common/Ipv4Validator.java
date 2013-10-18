/**
 * @author Sumanan sumanan@mit.edu
 * @since May 15, 2013
 */
package edu.mit.streamjit.impl.distributed.common;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Ipv4Validator {

	private Pattern pattern;
	private Matcher matcher;

	private static Ipv4Validator instance;

	private static final String IPADDRESS_PATTERN = "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\."
			+ "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\."
			+ "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\."
			+ "([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";

	public static Ipv4Validator getInstance() {
		if (instance == null)
			instance = new Ipv4Validator();
		return Ipv4Validator.instance;
	}

	private Ipv4Validator() {
		pattern = Pattern.compile(IPADDRESS_PATTERN);
	}

	public boolean isValid(String ipv4Address) {
		matcher = pattern.matcher(ipv4Address);
		return matcher.matches();
	}

	public boolean isValid(int portNo) {
		if (portNo > 0 && portNo < 65535)
			return true;
		else
			return false;
	}
}
