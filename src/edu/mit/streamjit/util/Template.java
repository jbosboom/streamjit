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
package edu.mit.streamjit.util;

import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A very simple template engine. Variable references of the form ${foo} are
 * replaced with their mapped values.
 * <p/>
 * This class uses StringBuffer (rather than the preferred StringBuilder)
 * because Matcher does as well. (Really, it could use an arbitrary Appendable.)
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 9/3/2013
 */

public final class Template {
	private static final Pattern VAR_REF = Pattern.compile("\\$\\{([^{}]*)\\}");
	private final String template;
	public Template(String template) {
		this.template = template;
	}

	/**
	 * Reads a template from an InputStream.
	 * @see java.lang.Class#getResourceAsStream(java.lang.String)
	 * @param stream the stream to read from; closed if successfully read
	 * @return a template
	 */
	public static Template from(InputStream stream) {
		try (Reader reader = new InputStreamReader(stream, StandardCharsets.UTF_8)) {
			StringBuilder sb = new StringBuilder();
			char[] buffer = new char[4096];
			int read;
			while ((read = reader.read(buffer)) != -1)
				sb.append(buffer, 0, read);
			return new Template(sb.toString());
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}

	public void replace(Map<String, ?> values, StringBuffer sb) {
		Matcher m = VAR_REF.matcher(template);
		while (m.find()) {
			String var = m.group(1);
			if (!values.containsKey(var))
				throw new IllegalArgumentException(String.format("No replacement for %s; replacements are: %s", var, values));
			String replacement = String.valueOf(values.get(var));
			m.appendReplacement(sb, replacement);
		}
		m.appendTail(sb);
	}

	public void replace(Iterable<? extends Map<String, ?>> values, StringBuffer sb) {
		for (Map<String, ?> value : values)
			replace(value, sb);
	}

	public void replaceReflect(Object data, StringBuffer sb) {
		ImmutableSet<Field> fields = ReflectionUtils.getAllFields(data.getClass());
		Map<String, Object> map = new HashMap<>();
		for (Field f : fields) {
			f.setAccessible(true);
			String name = f.getName();
			if (!map.containsKey(name))
				try {
				map.put(name, f.get(data));
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
		}
		replace(map, sb);
	}

	public void replaceReflect(Iterable<?> data, StringBuffer sb) {
		for (Object datum : data)
			replaceReflect(datum, sb);
	}
}
