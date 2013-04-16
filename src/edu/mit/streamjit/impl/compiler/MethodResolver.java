package edu.mit.streamjit.impl.compiler;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.apps.fmradio.FMRadio;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.common.MethodNodeBuilder;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.compiler.insts.ArrayLoadInst;
import edu.mit.streamjit.impl.compiler.insts.ArrayStoreInst;
import edu.mit.streamjit.impl.compiler.insts.BinaryInst;
import edu.mit.streamjit.impl.compiler.insts.BranchInst;
import edu.mit.streamjit.impl.compiler.insts.CallInst;
import edu.mit.streamjit.impl.compiler.insts.CastInst;
import edu.mit.streamjit.impl.compiler.insts.InstanceofInst;
import edu.mit.streamjit.impl.compiler.insts.JumpInst;
import edu.mit.streamjit.impl.compiler.insts.LoadInst;
import edu.mit.streamjit.impl.compiler.insts.NewArrayInst;
import edu.mit.streamjit.impl.compiler.insts.PhiInst;
import edu.mit.streamjit.impl.compiler.insts.ReturnInst;
import edu.mit.streamjit.impl.compiler.insts.StoreInst;
import edu.mit.streamjit.impl.compiler.insts.SwitchInst;
import edu.mit.streamjit.impl.compiler.types.ArrayType;
import edu.mit.streamjit.impl.compiler.types.MethodType;
import edu.mit.streamjit.impl.compiler.types.ReferenceType;
import edu.mit.streamjit.impl.compiler.types.ReturnType;
import edu.mit.streamjit.impl.compiler.types.Type;
import edu.mit.streamjit.impl.compiler.types.TypeFactory;
import edu.mit.streamjit.impl.compiler.types.VoidType;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.FieldInsnNode;
import org.objectweb.asm.tree.FrameNode;
import org.objectweb.asm.tree.IincInsnNode;
import org.objectweb.asm.tree.InsnList;
import org.objectweb.asm.tree.InsnNode;
import org.objectweb.asm.tree.IntInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.JumpInsnNode;
import org.objectweb.asm.tree.LabelNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.LookupSwitchInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.MultiANewArrayInsnNode;
import org.objectweb.asm.tree.TableSwitchInsnNode;
import org.objectweb.asm.tree.TypeInsnNode;
import org.objectweb.asm.tree.VarInsnNode;

/**
 * Resolves methods.
 *
 * This class assumes it's parsing valid bytecode, so it asserts rather than
 * throws on simple checks like "aload_0 is loading a reference type".
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/12/2013
 */
public final class MethodResolver {
	public static void resolve(Method m) {
		new MethodResolver(m).resolve();
	}

	private final Method method;
	private final MethodNode methodNode;
	private final List<BBInfo> blocks = new ArrayList<>();
	private final Module module;
	private final TypeFactory typeFactory;
	/**
	 * If we're resolving a constructor, this is the uninitializedThis value.
	 */
	private final UninitializedValue uninitializedThis;
	/**
	 * Used for generating sequential names (e.g., uninitialized object names).
	 */
	private int counter = 1;
	private MethodResolver(Method m) {
		this.method = m;
		this.module = method.getParent().getParent();
		this.typeFactory = module.types();
		try {
			this.methodNode = MethodNodeBuilder.buildMethodNode(method);
		} catch (IOException | NoSuchMethodException ex) {
			throw new RuntimeException(ex);
		}
		if (m.isConstructor())
			this.uninitializedThis = new UninitializedValue(typeFactory.getType(m.getParent()), "uninitializedThis");
		else
			this.uninitializedThis = null;
	}

	private void resolve() {
		findBlockBoundaries();

		//Process blocks such that at least one predecessor has already been
		//visited.  (We only process a block once; we add phi instructions when
		//frame merging and replace uses of the previous values in the block.)
		Set<BBInfo> visited = new HashSet<>();
		Queue<BBInfo> worklist = new ArrayDeque<>();
		worklist.add(blocks.get(0));
		while (!worklist.isEmpty()) {
			BBInfo block = worklist.remove();
			buildInstructions(block);
			visited.add(block);
			for (BasicBlock b : block.block.successors()) {
				for (BBInfo bi : blocks)
					if (bi.block == b) {
						if (!visited.contains(bi))
							worklist.add(bi);
						break;
					}
			}
		}
		//I'm assuming there's no trivially dead blocks.
		assert visited.size() == blocks.size();
	}

	private void findBlockBoundaries() {
		InsnList insns = methodNode.instructions;
		//We find the indices of any block-ending instruction and of any jump
		//target, sort, remove duplicates, then use pairs to define blocks. Note
		//these are end-exclusive indices, thus one after the block-enders, but
		//right on the jump targets (they're one-past-the-end of the preceding
		//block).
		List<Integer> indices = new ArrayList<>();
		indices.add(0);
		for (int i = 0; i < insns.size(); ++i) {
			AbstractInsnNode insn = insns.get(i);
			int opcode = insn.getOpcode();
			if (insn instanceof JumpInsnNode || insn instanceof LookupSwitchInsnNode ||
					insn instanceof TableSwitchInsnNode || opcode == Opcodes.ATHROW ||
					opcode == Opcodes.IRETURN || opcode == Opcodes.LRETURN ||
					opcode == Opcodes.FRETURN || opcode == Opcodes.DRETURN ||
					opcode == Opcodes.ARETURN || opcode == Opcodes.RETURN) {
				indices.add(i+1);
			}
			if (insn instanceof JumpInsnNode)
				indices.add(insns.indexOf(((JumpInsnNode)insn).label));
			else if (insn instanceof LookupSwitchInsnNode) {
				indices.add(insns.indexOf(((LookupSwitchInsnNode)insn).dflt));
				for (Object label : ((LookupSwitchInsnNode)insn).labels)
					indices.add(insns.indexOf((LabelNode)label));
			} else if (insn instanceof TableSwitchInsnNode) {
				indices.add(insns.indexOf(((TableSwitchInsnNode)insn).dflt));
				for (Object label : ((TableSwitchInsnNode)insn).labels)
					indices.add(insns.indexOf((LabelNode)label));
			}
		}

		//Remove duplicates and sort via TreeSet.
		indices = new ArrayList<>(new TreeSet<>(indices));
		for (int i = 1; i < indices.size(); ++i)
			blocks.add(new BBInfo(indices.get(i-1), indices.get(i)));
	}

	private void buildInstructions(BBInfo block) {
		FrameState frame = block.entryState.copy();
		for (int i = block.start; i < block.end; ++i) {
			AbstractInsnNode insn = methodNode.instructions.get(i);
			if (insn.getOpcode() == -1) continue;//pseudo-instruction node
			if (insn instanceof FieldInsnNode)
				interpret((FieldInsnNode)insn, frame, block);
			else if (insn instanceof IincInsnNode)
				interpret((IincInsnNode)insn, frame, block);
			else if (insn instanceof InsnNode)
				interpret((InsnNode)insn, frame, block);
			else if (insn instanceof IntInsnNode)
				interpret((IntInsnNode)insn, frame, block);
			else if (insn instanceof InvokeDynamicInsnNode)
				interpret((InvokeDynamicInsnNode)insn, frame, block);
			else if (insn instanceof JumpInsnNode)
				interpret((JumpInsnNode)insn, frame, block);
			else if (insn instanceof LdcInsnNode)
				interpret((LdcInsnNode)insn, frame, block);
			else if (insn instanceof LookupSwitchInsnNode)
				interpret((LookupSwitchInsnNode)insn, frame, block);
			else if (insn instanceof MethodInsnNode)
				interpret((MethodInsnNode)insn, frame, block);
			else if (insn instanceof MultiANewArrayInsnNode)
				interpret((MultiANewArrayInsnNode)insn, frame, block);
			else if (insn instanceof TableSwitchInsnNode)
				interpret((TableSwitchInsnNode)insn, frame, block);
			else if (insn instanceof TypeInsnNode)
				interpret((TypeInsnNode)insn, frame, block);
			else if (insn instanceof VarInsnNode)
				interpret((VarInsnNode)insn, frame, block);
		}

		//If the block doesn't have a TerminatorInst, add a JumpInst to the
		//fallthrough block.  (This occurs when blocks begin due to being a
		//jump target rather than due to a terminator opcode.)
		if (block.block.getTerminator() == null)
			block.block.instructions().add(new JumpInst(blocks.get(blocks.indexOf(block)+1).block));

		for (BasicBlock b : block.block.successors())
			for (BBInfo bi : blocks)
				if (bi.block == b) {
					merge(block, frame, bi);
					break;
				}
	}

	private void interpret(FieldInsnNode insn, FrameState frame, BBInfo block) {
		Klass k = getKlassByInternalName(insn.owner);
		Field f = k.getField(insn.name);
		switch (insn.getOpcode()) {
			case Opcodes.GETSTATIC:
				LoadInst li = new LoadInst(f);
				block.block.instructions().add(li);
				frame.stack.push(li);
				break;
			case Opcodes.GETFIELD:
				LoadInst li2 = new LoadInst(f, frame.stack.pop());
				block.block.instructions().add(li2);
				frame.stack.push(li2);
				break;
			case Opcodes.PUTSTATIC:
				StoreInst si = new StoreInst(f, frame.stack.pop());
				block.block.instructions().add(si);
				break;
			case Opcodes.PUTFIELD:
				StoreInst si2 = new StoreInst(f, frame.stack.pop(), frame.stack.pop());
				block.block.instructions().add(si2);
				break;
			default:
				throw new UnsupportedOperationException(""+insn.getOpcode());
		}
	}
	private void interpret(IincInsnNode insn, FrameState frame, BBInfo block) {
		switch (insn.getOpcode()) {
			case Opcodes.IINC:
				Constant<Integer> c = module.constants().getConstant(insn.incr);
				BinaryInst bi = new BinaryInst(frame.locals[insn.var], BinaryInst.Operation.ADD, c);
				block.block.instructions().add(bi);
				frame.locals[insn.var] = bi;
				break;
			default:
				throw new UnsupportedOperationException(""+insn.getOpcode());
		}
	}
	private void interpret(InsnNode insn, FrameState frame, BBInfo block) {
		ReturnType returnType = block.block.getParent().getType().getReturnType();
		switch (insn.getOpcode()) {
			case Opcodes.NOP:
				break;
			//<editor-fold defaultstate="collapsed" desc="Stack manipulation opcodes (pop, dup, swap)">
			case Opcodes.POP:
				assert categoryOf(frame.stack.peek().getType()) == 1;
				frame.stack.pop();
				break;
			case Opcodes.POP2:
				final int[][][] pop2Permutations = {
					{{1,1}, {}},
					{{2}, {}}
				};
				conditionallyPermute(frame, pop2Permutations);
				break;
			case Opcodes.DUP:
				final int[][][] dupPermutations = {{{1}, {1,1}}};
				conditionallyPermute(frame, dupPermutations);
				break;
			case Opcodes.DUP_X1:
				final int[][][] dup_x1Permutations = {{{1,1}, {1,2,1}}};
				conditionallyPermute(frame, dup_x1Permutations);
				break;
			case Opcodes.DUP_X2:
				final int[][][] dup_x2Permutations = {
					{{1,1,1}, {1,3,2,1}},
					{{1,2}, {1,2,1}}
				};
				conditionallyPermute(frame, dup_x2Permutations);
				break;
			case Opcodes.DUP2:
				final int[][][] dup2Permutations = {
					{{1,1}, {2,1,2,1}},
					{{2}, {1,1}}
				};
				conditionallyPermute(frame, dup2Permutations);
				break;
			case Opcodes.DUP2_X1:
				final int[][][] dup2_x1Permutations = {
					{{1,1,1}, {2,1,3,2,1}},
					{{2,1}, {1,2,1}}
				};
				conditionallyPermute(frame, dup2_x1Permutations);
				break;
			case Opcodes.DUP2_X2:
				final int[][][] dup2_x2Permutations = {
					{{1,1,1,1}, {2,1,4,3,2,1}},
					{{2,1,1}, {1,3,2,1}},
					{{3,2,1}, {2,1,3,2,1}},
					{{2,2}, {1,2,1}}
				};
				conditionallyPermute(frame, dup2_x2Permutations);
				break;
			case Opcodes.SWAP:
				final int[][][] swapPermutations = {{{1,1}, {1,2}}};
				conditionallyPermute(frame, swapPermutations);
				break;
			//</editor-fold>
			//<editor-fold defaultstate="collapsed" desc="Constant-stacking opcodes (iconst_0, etc.; see also bipush, sipush)">
			case Opcodes.ACONST_NULL:
				frame.stack.push(module.constants().getNullConstant());
				break;
			case Opcodes.ICONST_M1:
				frame.stack.push(module.constants().getSmallestIntConstant(-1));
				break;
			case Opcodes.ICONST_0:
				frame.stack.push(module.constants().getSmallestIntConstant(0));
				break;
			case Opcodes.ICONST_1:
				frame.stack.push(module.constants().getSmallestIntConstant(1));
				break;
			case Opcodes.ICONST_2:
				frame.stack.push(module.constants().getSmallestIntConstant(2));
				break;
			case Opcodes.ICONST_3:
				frame.stack.push(module.constants().getSmallestIntConstant(3));
				break;
			case Opcodes.ICONST_4:
				frame.stack.push(module.constants().getSmallestIntConstant(4));
				break;
			case Opcodes.ICONST_5:
				frame.stack.push(module.constants().getSmallestIntConstant(5));
				break;
			case Opcodes.LCONST_0:
				frame.stack.push(module.constants().getConstant(0L));
				break;
			case Opcodes.LCONST_1:
				frame.stack.push(module.constants().getConstant(1L));
				break;
			case Opcodes.FCONST_0:
				frame.stack.push(module.constants().getConstant(0f));
				break;
			case Opcodes.FCONST_1:
				frame.stack.push(module.constants().getConstant(1f));
				break;
			case Opcodes.FCONST_2:
				frame.stack.push(module.constants().getConstant(2f));
				break;
			case Opcodes.DCONST_0:
				frame.stack.push(module.constants().getConstant(0d));
				break;
			case Opcodes.DCONST_1:
				frame.stack.push(module.constants().getConstant(1d));
				break;
			//</editor-fold>
			//<editor-fold defaultstate="collapsed" desc="Return opcodes">
			case Opcodes.IRETURN:
				assert returnType.isSubtypeOf(typeFactory.getType(int.class));
				assert frame.stack.peek().getType().isSubtypeOf(returnType);
				block.block.instructions().add(new ReturnInst(returnType, frame.stack.pop()));
				break;
			case Opcodes.LRETURN:
				assert returnType.isSubtypeOf(typeFactory.getType(long.class));
				assert frame.stack.peek().getType().isSubtypeOf(returnType);
				block.block.instructions().add(new ReturnInst(returnType, frame.stack.pop()));
				break;
			case Opcodes.FRETURN:
				assert returnType.isSubtypeOf(typeFactory.getType(float.class));
				assert frame.stack.peek().getType().isSubtypeOf(returnType);
				block.block.instructions().add(new ReturnInst(returnType, frame.stack.pop()));
				break;
			case Opcodes.DRETURN:
				assert returnType.isSubtypeOf(typeFactory.getType(double.class));
				assert frame.stack.peek().getType().isSubtypeOf(returnType);
				block.block.instructions().add(new ReturnInst(returnType, frame.stack.pop()));
				break;
			case Opcodes.ARETURN:
				assert returnType.isSubtypeOf(typeFactory.getType(Object.class));
				assert frame.stack.peek().getType().isSubtypeOf(returnType);
				block.block.instructions().add(new ReturnInst(returnType, frame.stack.pop()));
				break;
			case Opcodes.RETURN:
				assert returnType instanceof VoidType || method.isConstructor();
				block.block.instructions().add(new ReturnInst(returnType));
				break;
			//</editor-fold>
			//<editor-fold defaultstate="collapsed" desc="Binary math opcodes">
			case Opcodes.IADD:
			case Opcodes.LADD:
			case Opcodes.FADD:
			case Opcodes.DADD:
				binary(BinaryInst.Operation.ADD, frame, block);
				break;
			case Opcodes.ISUB:
			case Opcodes.LSUB:
			case Opcodes.FSUB:
			case Opcodes.DSUB:
				binary(BinaryInst.Operation.SUB, frame, block);
				break;
			case Opcodes.IMUL:
			case Opcodes.LMUL:
			case Opcodes.FMUL:
			case Opcodes.DMUL:
				binary(BinaryInst.Operation.MUL, frame, block);
				break;
			case Opcodes.IDIV:
			case Opcodes.LDIV:
			case Opcodes.FDIV:
			case Opcodes.DDIV:
				binary(BinaryInst.Operation.DIV, frame, block);
				break;
			case Opcodes.IREM:
			case Opcodes.LREM:
			case Opcodes.FREM:
			case Opcodes.DREM:
				binary(BinaryInst.Operation.REM, frame, block);
				break;
			case Opcodes.ISHL:
			case Opcodes.LSHL:
				binary(BinaryInst.Operation.SHL, frame, block);
				break;
			case Opcodes.ISHR:
			case Opcodes.LSHR:
				binary(BinaryInst.Operation.SHR, frame, block);
				break;
			case Opcodes.IUSHR:
			case Opcodes.LUSHR:
				binary(BinaryInst.Operation.USHR, frame, block);
				break;
			case Opcodes.IAND:
			case Opcodes.LAND:
				binary(BinaryInst.Operation.AND, frame, block);
				break;
			case Opcodes.IOR:
			case Opcodes.LOR:
				binary(BinaryInst.Operation.OR, frame, block);
				break;
			case Opcodes.IXOR:
			case Opcodes.LXOR:
				binary(BinaryInst.Operation.XOR, frame, block);
				break;
			case Opcodes.LCMP:
			case Opcodes.FCMPL:
			case Opcodes.DCMPL:
				binary(BinaryInst.Operation.CMP, frame, block);
				break;
			case Opcodes.FCMPG:
			case Opcodes.DCMPG:
				binary(BinaryInst.Operation.CMPG, frame, block);
				break;
			//</editor-fold>
			//<editor-fold defaultstate="collapsed" desc="Primitive casts">
			case Opcodes.I2L:
				cast(int.class, long.class, frame, block);
				break;
			case Opcodes.I2F:
				cast(int.class, float.class, frame, block);
				break;
			case Opcodes.I2D:
				cast(int.class, double.class, frame, block);
				break;
			case Opcodes.L2I:
				cast(long.class, int.class, frame, block);
				break;
			case Opcodes.L2F:
				cast(long.class, float.class, frame, block);
				break;
			case Opcodes.L2D:
				cast(long.class, double.class, frame, block);
				break;
			case Opcodes.F2I:
				cast(float.class, int.class, frame, block);
				break;
			case Opcodes.F2L:
				cast(float.class, long.class, frame, block);
				break;
			case Opcodes.F2D:
				cast(float.class, double.class, frame, block);
				break;
			case Opcodes.D2I:
				cast(double.class, int.class, frame, block);
				break;
			case Opcodes.D2L:
				cast(double.class, long.class, frame, block);
				break;
			case Opcodes.D2F:
				cast(double.class, float.class, frame, block);
				break;
			case Opcodes.I2B:
				cast(int.class, byte.class, frame, block);
				break;
			case Opcodes.I2C:
				cast(int.class, char.class, frame, block);
				break;
			case Opcodes.I2S:
				cast(int.class, short.class, frame, block);
				break;
			//</editor-fold>
			//<editor-fold defaultstate="collapsed" desc="Array store opcodes">
			case Opcodes.IASTORE:
			case Opcodes.LASTORE:
			case Opcodes.FASTORE:
			case Opcodes.DASTORE:
			case Opcodes.AASTORE:
			case Opcodes.BASTORE:
			case Opcodes.CASTORE:
			case Opcodes.SASTORE:
				Value data = frame.stack.pop();
				Value index = frame.stack.pop();
				Value array = frame.stack.pop();
				ArrayStoreInst asi = new ArrayStoreInst(array, index, data);
				block.block.instructions().add(asi);
				break;
			//</editor-fold>
			//<editor-fold defaultstate="collapsed" desc="Array load opcodes">
			case Opcodes.IALOAD:
			case Opcodes.LALOAD:
			case Opcodes.FALOAD:
			case Opcodes.DALOAD:
			case Opcodes.AALOAD:
			case Opcodes.BALOAD:
			case Opcodes.CALOAD:
			case Opcodes.SALOAD:
				Value index2 = frame.stack.pop();
				Value array2 = frame.stack.pop();
				ArrayLoadInst ali = new ArrayLoadInst(array2, index2);
				block.block.instructions().add(ali);
				frame.stack.push(ali);
				break;
			//</editor-fold>
			default:
				throw new UnsupportedOperationException(""+insn.getOpcode());
		}
	}
	private void interpret(IntInsnNode insn, FrameState frame, BBInfo block) {
		int operand = insn.operand;
		switch (insn.getOpcode()) {
			case Opcodes.NEWARRAY:
				ArrayType t;
				switch (operand) {
					case Opcodes.T_BOOLEAN:
						t = module.types().getArrayType(boolean[].class);
						break;
					case Opcodes.T_BYTE:
						t = module.types().getArrayType(byte[].class);
						break;
					case Opcodes.T_CHAR:
						t = module.types().getArrayType(char[].class);
						break;
					case Opcodes.T_SHORT:
						t = module.types().getArrayType(short[].class);
						break;
					case Opcodes.T_INT:
						t = module.types().getArrayType(int[].class);
						break;
					case Opcodes.T_LONG:
						t = module.types().getArrayType(long[].class);
						break;
					case Opcodes.T_FLOAT:
						t = module.types().getArrayType(float[].class);
						break;
					case Opcodes.T_DOUBLE:
						t = module.types().getArrayType(double[].class);
						break;
					default:
						throw new AssertionError(operand);
				}
				NewArrayInst i = new NewArrayInst(t, frame.stack.pop());
				block.block.instructions().add(i);
				frame.stack.push(i);
				break;
			default:
				throw new UnsupportedOperationException(""+insn.getOpcode());
		}
	}
	private void interpret(InvokeDynamicInsnNode insn, FrameState frame, BBInfo block) {
		switch (insn.getOpcode()) {
			default:
				throw new UnsupportedOperationException(""+insn.getOpcode());
		}
	}
	//<editor-fold defaultstate="collapsed" desc="JumpInsnNode (goto and branches)">
	private static final ImmutableMap<Integer, BranchInst.Sense> OPCODE_TO_SENSE = ImmutableMap.<Integer, BranchInst.Sense>builder()
			.put(Opcodes.IFEQ, BranchInst.Sense.EQ)
			.put(Opcodes.IFNE, BranchInst.Sense.NE)
			.put(Opcodes.IFLT, BranchInst.Sense.LT)
			.put(Opcodes.IFGE, BranchInst.Sense.GE)
			.put(Opcodes.IFGT, BranchInst.Sense.GT)
			.put(Opcodes.IFLE, BranchInst.Sense.LE)
			.put(Opcodes.IF_ICMPEQ, BranchInst.Sense.EQ)
			.put(Opcodes.IF_ICMPNE, BranchInst.Sense.NE)
			.put(Opcodes.IF_ICMPLT, BranchInst.Sense.LT)
			.put(Opcodes.IF_ICMPGE, BranchInst.Sense.GE)
			.put(Opcodes.IF_ICMPGT, BranchInst.Sense.GT)
			.put(Opcodes.IF_ICMPLE, BranchInst.Sense.LE)
			.put(Opcodes.IF_ACMPEQ, BranchInst.Sense.EQ)
			.put(Opcodes.IF_ACMPNE, BranchInst.Sense.NE)
			.put(Opcodes.IFNULL, BranchInst.Sense.EQ)
			.put(Opcodes.IFNONNULL, BranchInst.Sense.NE)
			.build();
	private void interpret(JumpInsnNode insn, FrameState frame, BBInfo block) {
		//All JumpInsnNodes have a target.  Find it.
		BBInfo target = blockByInsn(insn.label);
		assert target != null;

		if (insn.getOpcode() == Opcodes.GOTO) {
			block.block.instructions().add(new JumpInst(target.block));
			return;
		} else if (insn.getOpcode() == Opcodes.JSR)
			throw new UnsupportedOperationException("jsr not supported; upgrade to Java 6-era class files");

		//Remaining opcodes are branches.
		BBInfo fallthrough = blocks.get(blocks.indexOf(block)+1);
		BranchInst.Sense sense = OPCODE_TO_SENSE.get(insn.getOpcode());
		//The second operand may come from the stack or may be a constant 0 or null.
		Value right;
		switch (insn.getOpcode()) {
			case Opcodes.IFEQ:
			case Opcodes.IFNE:
			case Opcodes.IFLT:
			case Opcodes.IFGE:
			case Opcodes.IFGT:
			case Opcodes.IFLE:
				right = module.constants().getConstant(0);
				break;
			case Opcodes.IFNULL:
			case Opcodes.IFNONNULL:
				right = module.constants().getNullConstant();
				break;
			case Opcodes.IF_ICMPEQ:
			case Opcodes.IF_ICMPNE:
			case Opcodes.IF_ICMPLT:
			case Opcodes.IF_ICMPGE:
			case Opcodes.IF_ICMPGT:
			case Opcodes.IF_ICMPLE:
			case Opcodes.IF_ACMPEQ:
			case Opcodes.IF_ACMPNE:
				right = frame.stack.pop();
				break;
			default:
				throw new AssertionError("Can't happen! Branch opcode missing? "+insn.getOpcode());
		}
		//First operand always comes from the stack.
		Value left = frame.stack.pop();
		block.block.instructions().add(new BranchInst(left, sense, right, target.block, fallthrough.block));
	}
	//</editor-fold>
	private void interpret(LdcInsnNode insn, FrameState frame, BBInfo block) {
		assert insn.getOpcode() == Opcodes.LDC;
		ConstantFactory cf = module.constants();
		Object c = insn.cst;
		if (c instanceof Integer)
			frame.stack.push(cf.getSmallestIntConstant((Integer)c));
		else if (c instanceof Long)
			frame.stack.push(cf.getConstant((Long)c));
		else if (c instanceof Float)
			frame.stack.push(cf.getConstant((Float)c));
		else if (c instanceof Double)
			frame.stack.push(cf.getConstant((Double)c));
		else if (c instanceof String)
			frame.stack.push(cf.getConstant((String)c));
		else if (c instanceof org.objectweb.asm.Type) {
			org.objectweb.asm.Type t = (org.objectweb.asm.Type)c;
			Constant<Class<?>> d = cf.getConstant(getKlassByInternalName(t.getInternalName()).getBackingClass());
			frame.stack.push(d);
		} else
			throw new AssertionError(c);
	}
	private void interpret(LookupSwitchInsnNode insn, FrameState frame, BBInfo block) {
		assert insn.getOpcode() == Opcodes.LOOKUPSWITCH;
		ConstantFactory cf = module.constants();
		SwitchInst inst = new SwitchInst(frame.stack.pop(), blockByInsn(insn.dflt).block);
		for (int i = 0; i < insn.keys.size(); ++i)
			inst.put(cf.getConstant((Integer)insn.keys.get(i)), blockByInsn((LabelNode)insn.labels.get(i)).block);
		block.block.instructions().add(inst);
	}
	private void interpret(MethodInsnNode insn, FrameState frame, BBInfo block) {
		Klass k = getKlassByInternalName(insn.owner);
		MethodType mt = typeFactory.getMethodType(insn.desc);
		Method m;
		if (insn.getOpcode() == Opcodes.INVOKESTATIC)
			m = k.getMethod(insn.name, mt);
		else if (insn.getOpcode() == Opcodes.INVOKESPECIAL && insn.name.equals("<init>")) {
			//TODO: invokespecial rules are more complex than this
			//We consider constructors to return their type.
			mt = mt.withReturnType(typeFactory.getType(k));
			m = k.getMethod(insn.name, mt);
		} else {
			//The receiver argument is not in the descriptor, but we represent it in
			//the IR type system.
			if (insn.getOpcode() != Opcodes.INVOKESTATIC)
				mt = mt.prependArgument(typeFactory.getRegularType(k));
			m = k.getMethodByVirtual(insn.name, mt);
		}
		CallInst inst = new CallInst(m);
		block.block.instructions().add(inst);

		//Args are pushed from left-to-right, popped from right-to-left.
		for (int i = mt.getParameterTypes().size()-1; i >= 0; --i)
			inst.setArgument(i, frame.stack.pop());

		//If we called a ctor, we have an uninit object on the stack.  Replace
		//it with the constructed object, or with uninitializedThis if we're a
		//ctor ourselves and we called a superclass ctor.
		if (m.isConstructor()) {
			//TODO: always a direct superclass? seems likely but unsure.
			Value replacement = method.isConstructor() && method.getParent().getSuperclass().equals(k)
					? uninitializedThis : inst;
			Value toBeReplaced = frame.stack.pop();
			assert toBeReplaced instanceof UninitializedValue;
			frame.replace(toBeReplaced, replacement);
		} else if (!(mt.getReturnType() instanceof VoidType))
			frame.stack.push(inst);
	}
	private void interpret(MultiANewArrayInsnNode insn, FrameState frame, BBInfo block) {
		switch (insn.getOpcode()) {
			default:
				throw new UnsupportedOperationException(""+insn.getOpcode());
		}
	}
	private void interpret(TableSwitchInsnNode insn, FrameState frame, BBInfo block) {
		switch (insn.getOpcode()) {
			default:
				throw new UnsupportedOperationException(""+insn.getOpcode());
		}
	}
	private void interpret(TypeInsnNode insn, FrameState frame, BBInfo block) {
		Klass k = getKlassByInternalName(insn.desc);
		ReferenceType t = typeFactory.getReferenceType(k);
		switch (insn.getOpcode()) {
			case Opcodes.NEW:
				frame.stack.push(new UninitializedValue(t, "uninit"+(counter++)));
				break;
			case Opcodes.CHECKCAST:
				CastInst c = new CastInst(t, frame.stack.pop());
				block.block.instructions().add(c);
				frame.stack.push(c);
				break;
			case Opcodes.INSTANCEOF:
				InstanceofInst ioi = new InstanceofInst(t, frame.stack.pop());
				block.block.instructions().add(ioi);
				frame.stack.push(ioi);
				break;
			case Opcodes.ANEWARRAY:
				ArrayType at = typeFactory.getArrayType(module.getArrayKlass(k, 1));
				NewArrayInst nai = new NewArrayInst(at, frame.stack.pop());
				block.block.instructions().add(nai);
				frame.stack.push(nai);
				break;
			default:
				throw new UnsupportedOperationException(""+insn.getOpcode());
		}
	}
	private void interpret(VarInsnNode insn, FrameState frame, BBInfo block) {
		int var = insn.var;
		switch (insn.getOpcode()) {
			case Opcodes.ILOAD:
				assert frame.locals[var].getType().isSubtypeOf(typeFactory.getType(int.class));
				frame.stack.push(frame.locals[var]);
				break;
			case Opcodes.LLOAD:
				assert frame.locals[var].getType().isSubtypeOf(typeFactory.getType(long.class));
				frame.stack.push(frame.locals[var]);
				break;
			case Opcodes.FLOAD:
				assert frame.locals[var].getType().isSubtypeOf(typeFactory.getType(float.class));
				frame.stack.push(frame.locals[var]);
				break;
			case Opcodes.DLOAD:
				assert frame.locals[var].getType().isSubtypeOf(typeFactory.getType(double.class));
				frame.stack.push(frame.locals[var]);
				break;
			case Opcodes.ALOAD:
				assert frame.locals[var].getType() instanceof ReferenceType;
				frame.stack.push(frame.locals[var]);
				break;
			case Opcodes.ISTORE:
				assert frame.stack.peek().getType().isSubtypeOf(typeFactory.getType(int.class));
				frame.locals[var] = frame.stack.pop();
				break;
			case Opcodes.LSTORE:
				assert frame.stack.peek().getType().isSubtypeOf(typeFactory.getType(long.class));
				frame.locals[var] = frame.stack.pop();
				break;
			case Opcodes.FSTORE:
				assert frame.stack.peek().getType().isSubtypeOf(typeFactory.getType(float.class));
				frame.locals[var] = frame.stack.pop();
				break;
			case Opcodes.DSTORE:
				assert frame.stack.peek().getType().isSubtypeOf(typeFactory.getType(double.class));
				frame.locals[var] = frame.stack.pop();
				break;
			case Opcodes.ASTORE:
				assert frame.stack.peek().getType() instanceof ReferenceType;
				frame.locals[var] = frame.stack.pop();
				break;
			default:
				throw new UnsupportedOperationException(""+insn.getOpcode());
		}
	}

	private Klass getKlassByInternalName(String internalName) {
		String binaryName = internalName.replace('/', '.');
		Klass k = module.getKlass(binaryName);
		if (k != null)
			return k;

		Class<?> c = null;
		try {
			c = Class.forName(binaryName);
		} catch (ClassNotFoundException ex) {
			Thread.currentThread().stop(ex);
		}
		return module.getKlass(c);
	}

	//<editor-fold defaultstate="collapsed" desc="Stack manipulation opcodes support">
	/**
	 * Conditionally permutes the values on the operand stack in the given
	 * frame. The permutations are given as an array of 2-element arrays, the
	 * first element of which specifies the condition as a constraint on the
	 * categories of the stacked operand types, with the top of the stack
	 * beginning at index 0, and the second element of which specifies 1-based
	 * indices giving the resulting permutation, with the element at index 0
	 * being towards the bottom of the stack (pushed first). (This matches the
	 * instruction descriptions in the JVMS.)
	 *
	 * Strictly speaking, the permutations need not be permutations; they may
	 * contain duplicate or dropped indices.
	 *
	 * Only one permutation will be applied.  If no permutation matches, an
	 * AssertionError is thrown.
	 *
	 * This is used for the implementation of the DUP instruction family.
	 * @param frame the frame containing the stack to permute
	 * @param permutations the conditional permutations to apply
	 */
	private void conditionallyPermute(FrameState frame, int[][][] permutations) {
		for (int[][] permutation : permutations) {
			int[] categories = permutation[0];
			if (Arrays.equals(categories, categoriesOnStack(frame, categories.length))) {
				Value[] v = new Value[categories.length];
				for (int i = 0; i < v.length; ++i)
					v[i] = frame.stack.pop();
				for (int i : permutation[1])
					frame.stack.push(v[i-1]);
				return;
			}
		}
		throw new AssertionError("no permutations matched");
	}

	/**
	 * Returns an array containing the categories of the first n values on the
	 * operand stack of the given frame.  If there are fewer values, the rest
	 * are filled with -1.
	 * @param frame the frame to check categories on stack of
	 * @return an array of categories
	 */
	private int[] categoriesOnStack(FrameState frame, int n) {
		int[] x = new int[n];
		Arrays.fill(x, -1);
		Value[] v = frame.stack.toArray(new Value[0]);
		for (int i = 0; i < v.length && i < x.length; ++i)
			x[i] = categoryOf(v[i].getType());
		return x;
	}
	//</editor-fold>

	private void binary(BinaryInst.Operation operation, FrameState frame, BBInfo block) {
		Value right = frame.stack.pop();
		Value left = frame.stack.pop();
		BinaryInst inst = new BinaryInst(left, operation, right);
		block.block.instructions().add(inst);
		frame.stack.push(inst);
	}

	private void cast(Class<?> from, Class<?> to, FrameState frame, BBInfo block) {
		Type targetType = typeFactory.getType(to);
		CastInst c = new CastInst(targetType, frame.stack.pop());
		block.block.instructions().add(c);
		frame.stack.push(c);
	}

	private BBInfo blockByInsn(AbstractInsnNode insn) {
		int targetIdx = methodNode.instructions.indexOf(insn);
		for (BBInfo b : blocks)
			if (b.start <= targetIdx && targetIdx < b.end)
				return b;
		return null;
	}

	/**
	 * Merge the given frame state with the entry state of the given block,
	 * RAUW-ing as required if phi instructions are inserted.
	 * @param p the final frame state of a predecessor
	 * @param s the successor block
	 */
	private void merge(BBInfo predecessor, FrameState p, BBInfo s) {
		if (s.entryState == null) {
			//If this state didn't have a frame, it has only one predecessor, so
			//just use our state.
			s.entryState = p.copy();
			return;
		}

		//This block has multiple predecessors, so it had a frame, so we gave it
		//phi instructions in its entry state.
		for (int i = 0; i < p.locals.length; ++i) {
			//If we're null, we don't have a definition.
			if (p.locals[i] == null)
				continue;
			//We might not unify with the other predecessors.
			if (!(s.entryState.locals[i] instanceof PhiInst))
				continue;
			//Otherwise, register our definition.
			((PhiInst)s.entryState.locals[i]).put(predecessor.block, p.locals[i]);
		}
		Iterator<Value> us = p.stack.iterator(), them = s.entryState.stack.iterator();
		while (us.hasNext()) {
			Value theirVal = them.next(), ourVal = us.next();
			if (theirVal instanceof PhiInst)
				((PhiInst)theirVal).put(predecessor.block, ourVal);
		}
	}

	private final class BBInfo {
		private final BasicBlock block;
		//The index of the first and one-past-the-last instructions.
		private final int start, end;
		private FrameState entryState;
		private final FrameNode frame;
		private BBInfo(int start, int end) {
			this.block = new BasicBlock(method.getParent().getParent());
			method.basicBlocks().add(this.block);
			this.start = start;
			this.end = end;
			if (start == 0) { //first block starts with args and empty stack
				this.entryState = new FrameState(methodNode.maxLocals);
				Value[] entryLocals = entryState.locals;
				int i = 0;
				//If the method is a constructor, it begins with an
				//UninitializedThis object in local variable 0.
				if (method.isConstructor())
					entryLocals[i++] = uninitializedThis;
				for (Argument a : method.arguments()) {
					entryLocals[i] = a;
					Type argType = a.getType();
					i += categoryOf(argType);
				}
			}
			this.frame = findOnlyFrameNode();
			if (this.frame != null)
				entryStateFromFrame();
		}

		private FrameNode findOnlyFrameNode() {
			FrameNode f = null;
			for (int i = start; i != end; ++i) {
				AbstractInsnNode insn = methodNode.instructions.get(i);
				if (insn instanceof FrameNode) {
					assert f == null : f + " " +insn;
					f = (FrameNode)insn;
				}
			}
			return f;
		}

		private void entryStateFromFrame() {
			this.entryState = new FrameState(methodNode.maxLocals);
			valueArrayFromFrameList(frame.local, entryState.locals, true);
			Value[] stack = new Value[frame.stack.size()];
			valueArrayFromFrameList(frame.stack, stack, false);
			for (Value v : stack)
				entryState.stack.push(v);

			//Attach those PhiInsts.
			for (Value v : this.entryState.locals)
				if (v instanceof PhiInst)
					block.instructions().add((PhiInst)v);
			for (Value v : this.entryState.stack)
				if (v instanceof PhiInst)
					block.instructions().add((PhiInst)v);
		}

		private void valueArrayFromFrameList(List<?> frameList, Value[] values, boolean expandCat2Types) {
			for (int i = 0; i < frameList.size(); ++i) {
				Object o = frameList.get(i);
				if (o instanceof Integer) {
					Integer t = (Integer)o;
					if (t.equals(Opcodes.INTEGER))
						values[i] = new PhiInst(typeFactory.getType(int.class));
					else if (t.equals(Opcodes.FLOAT))
						values[i] = new PhiInst(typeFactory.getType(float.class));
					else if (t.equals(Opcodes.DOUBLE)) {
						values[i] = new PhiInst(typeFactory.getType(double.class));
						if (expandCat2Types)
							++i; //two local slots
					} else if (t.equals(Opcodes.LONG)) {
						values[i] = new PhiInst(typeFactory.getType(long.class));
						if (expandCat2Types)
							++i; //two local slots
					} else if (t.equals(Opcodes.NULL))
						values[i] = module.constants().getNullConstant();
					else if (t.equals(Opcodes.UNINITIALIZED_THIS)) {
						assert uninitializedThis != null;
						values[i] = uninitializedThis;
					}
					//TOP ignored
				} else if (o instanceof String) {
					Type t = typeFactory.getType(getKlassByInternalName((String)o));
					values[i] = new PhiInst(t);
				} //else Label (uninit -- ignored)
			}
		}
	}

	/**
	 * Returns the "category" of the given type, the number of stack or local
	 * slots it occupies in a frame.
	 * @param t a type
	 * @return the type's category
	 */
	private int categoryOf(Type t) {
		return (t.equals(typeFactory.getType(long.class)) || t.equals(typeFactory.getType(double.class))) ? 2 : 1;
	}

	private final class FrameState {
		private final Value[] locals;
		private final Deque<Value> stack;
		private FrameState(int localSize) {
			this.locals = new Value[localSize];
			this.stack = new ArrayDeque<>();
		}
		private FrameState copy() {
			FrameState s = new FrameState(locals.length);
			System.arraycopy(locals, 0, s.locals, 0, locals.length);
			return s;
		}
		private void replace(Value toBeReplaced, Value replacement) {
			for (int i = 0; i < locals.length; ++i)
				if (toBeReplaced.equals(locals[i]))
					locals[i] = replacement;
			//Best we can do with a deque.
			Value[] v = stack.toArray(new Value[0]);
			for (int i = 0; i < v.length; ++i)
				if (toBeReplaced.equals(v[i]))
					v[i] = replacement;
			stack.clear();
			for (Value x : v)
				stack.add(x);
		}
		@Override
		public String toString() {
			return "Locals: "+Arrays.toString(locals)+", Stack: "+stack.toString();
		}
	}

	/**
	 * A dummy value used when building SSA form.  Exists only to get RAUW'd to
	 * the result of the constructor call.
	 *
	 * Has the type of the object under construction.
	 */
	private static class UninitializedValue extends Value {
		private UninitializedValue(Type type, String name) {
			super(type, name);
		}
	}

	public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
		Class<?> sgc = Class.forName("edu.mit.streamjit.apps.fft5.FFT5$FFT5Kernel");
		Constructor<?> ctor = sgc.getDeclaredConstructor();
		ctor.setAccessible(true);
		OneToOneElement<?, ?> sgh = (OneToOneElement<?, ?>)ctor.newInstance();
		ConnectWorkersVisitor cwv = new ConnectWorkersVisitor();
		sgh.visit(cwv);
		ImmutableSet<Worker<?, ?>> workers = Workers.getAllWorkersInGraph(cwv.getSource());

		Module m = new Module();
		for (Worker<?, ?> w : workers) {
			Klass k = m.getKlass(w.getClass());
			for (Method method : k.methods())
				if (!method.isResolved() && method.isResolvable()) {
					method.resolve();
					System.out.println("Resolved "+k.getName()+" "+method);
				}
		}
	}
}
