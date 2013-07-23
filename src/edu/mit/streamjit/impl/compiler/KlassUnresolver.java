package edu.mit.streamjit.impl.compiler;

import static com.google.common.base.Preconditions.*;
import java.io.PrintWriter;
import java.util.Arrays;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FieldNode;
import org.objectweb.asm.util.CheckClassAdapter;

/**
 * Builds a .class file (as a byte[]) from a Klass.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/17/2013
 */
public final class KlassUnresolver {
	public static byte[] unresolve(Klass k) {
		checkNotNull(k);
		//TODO: permit this for testing
		//checkArgument(k.isMutable());
		return new KlassUnresolver(k).unresolve();
	}

	private final Klass klass;
	private final ClassNode classNode;
	private KlassUnresolver(Klass k) {
		this.klass = k;
		this.classNode = new ClassNode(Opcodes.ASM4);
	}

	@SuppressWarnings("unchecked")
	private byte[] unresolve() {
		this.classNode.version = Opcodes.V1_7;
		this.classNode.access = Modifier.toBits(klass.modifiers());
		this.classNode.name = internalName(klass);
		assert klass.getSuperclass() != null || Object.class.equals(klass.getBackingClass()) : klass;
		this.classNode.superName = internalName(klass.getSuperclass());
		for (Klass k : klass.interfaces())
			this.classNode.interfaces.add(internalName(k));

		for (Field f : klass.fields()) {
			FieldNode fn = new FieldNode(Opcodes.ASM4,
					Modifier.toBits(f.modifiers()),
					f.getName(),
					f.getType().getFieldType().getDescriptor(),
					null, null);
			this.classNode.fields.add(fn);
		}

		for (Method m : klass.methods())
			this.classNode.methods.add(MethodUnresolver.unresolve(m));

		ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS | ClassWriter.COMPUTE_FRAMES);
		ClassVisitor cv = cw;
		boolean assertionsEnabled = false;
		assert assertionsEnabled = true; //intentional side effect
		if (assertionsEnabled)
			cv = new CheckClassAdapter(cv, true);
		classNode.accept(cv);
		return cw.toByteArray();
	}

	private String internalName(Klass k) {
		return k.getName().replace('.', '/');
	}

	public static void main(String[] args) {
		Module m = new Module();
		Klass k = m.getKlass(Module.class);
		for (Method method : k.methods())
			if (method.isResolvable()) {
				method.resolve();
				method.dump(new PrintWriter(System.out, true));
			}
		byte[] b = unresolve(k);
		System.out.println(Arrays.toString(b));
		System.out.println(b.length);
	}
}
