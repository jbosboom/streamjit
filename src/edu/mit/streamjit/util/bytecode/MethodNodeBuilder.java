package edu.mit.streamjit.util.bytecode;

import edu.mit.streamjit.util.bytecode.types.MethodType;
import java.io.IOException;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.MethodNode;

/**
 * Builds a MethodNode for the method with the given name and descriptor.
 */
public final class MethodNodeBuilder {
	private MethodNodeBuilder() {}
	public static MethodNode buildMethodNode(Class<?> klass, String methodName, String methodDescriptor) throws IOException, NoSuchMethodException {
		ClassReader r = new ClassReader(klass.getName());
		MethodNodeBuildingClassVisitor mnbcv = new MethodNodeBuildingClassVisitor(methodName, methodDescriptor);
		r.accept(mnbcv, ClassReader.EXPAND_FRAMES);
		MethodNode methodNode = mnbcv.getMethodNode();
		if (methodNode == null)
			throw new NoSuchMethodException(klass.getName() + "#" + methodName + methodDescriptor);
		return methodNode;
	}

	public static MethodNode buildMethodNode(Method method) throws IOException, NoSuchMethodException {
		Class<?> klass = method.getParent().getBackingClass();
		String methodName = method.getName();
		MethodType internalType = method.getType();
		//Methods taking a this parameter have it explicitly represented in
		//their MethodType, but the JVM doesn't specify it in the method
		//descriptor.
		if (method.hasReceiver())
			internalType = internalType.dropFirstArgument();
		//We consider constructors to return their class (because the CallInst
		//defines a Value of that type), but the JVM thinks they return void.
		if (method.isConstructor())
			internalType = internalType.withReturnType(internalType.getTypeFactory().getType(void.class));
		String methodDescriptor = internalType.getDescriptor();
		return buildMethodNode(klass, methodName, methodDescriptor);
	}

	private static final class MethodNodeBuildingClassVisitor extends ClassVisitor {
		private final String name;
		private final String descriptor;
		private MethodNode mn;
		private MethodNodeBuildingClassVisitor(String name, String descriptor) {
			super(Opcodes.ASM4);
			this.name = name;
			this.descriptor = descriptor;
		}

		@Override
		public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
			if (name.equals(this.name) && desc.equals(this.descriptor)) {
				mn = new MethodNode(Opcodes.ASM4, access, name, desc, signature, exceptions);
				return mn;
			}
			return null;
		}

		public MethodNode getMethodNode() {
			return mn;
		}
	}
}
