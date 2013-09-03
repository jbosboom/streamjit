package edu.mit.streamjit.util;

import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Field;
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
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 9/3/2013
 */

public final class Template {
	private static final Pattern VAR_REF = Pattern.compile("\\$\\{([^{}]*)\\}");
	private final String template;
	public Template(String template) {
		this.template = template;
	}

	public void replace(Map<String, ?> values, StringBuffer sb) {
		Matcher m = VAR_REF.matcher(template);
		while (m.find()) {
			String replacement = String.valueOf(values.get(m.group(1)));
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
