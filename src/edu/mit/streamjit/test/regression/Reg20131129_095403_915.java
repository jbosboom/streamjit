package edu.mit.streamjit.test.regression;

import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmarker;
import edu.mit.streamjit.test.Datasets;
import java.util.Collections;
import java.util.List;

/**
 * Triggers OutOfMemoryError.  It's a pathological case of rate mismatches, so
 * I'm not too worried.
 * @since 11/29/2013 9:54PM EST
 */
//@ServiceProvider(Benchmark.class)
public class Reg20131129_095403_915 implements Benchmark {
	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public OneToOneElement<Object, Object> instantiate() {
		return new Pipeline(new OneToOneElement[]{
			new Pipeline(new OneToOneElement[]{
				new edu.mit.streamjit.impl.common.TestFilters.Adder(20),
				new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(3), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
					new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(4), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
						new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(2), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
							new edu.mit.streamjit.impl.common.TestFilters.PeekingAdder(10),
							new edu.mit.streamjit.impl.common.TestFilters.Multiplier(3),
							new edu.mit.streamjit.impl.common.TestFilters.Multiplier(100),
							new edu.mit.streamjit.api.Identity()
						}),
						new edu.mit.streamjit.impl.common.TestFilters.ArrayListHasher(1)
					}),
					new edu.mit.streamjit.impl.common.TestFilters.Adder(20)
				}),
				new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
					new edu.mit.streamjit.impl.common.TestFilters.ArrayListHasher(2)
				}),
				new Pipeline(new OneToOneElement[]{
					new edu.mit.streamjit.impl.common.TestFilters.Batcher(2),
					new edu.mit.streamjit.impl.common.TestFilters.Batcher(10),
					new Pipeline(new OneToOneElement[]{
						new edu.mit.streamjit.impl.common.TestFilters.PeekingAdder(10),
						new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
							new edu.mit.streamjit.impl.common.TestFilters.Adder(20),
							new edu.mit.streamjit.impl.common.TestFilters.PeekingAdder(10),
							new edu.mit.streamjit.impl.common.TestFilters.Multiplier(100)
						}),
						new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(3), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
							new edu.mit.streamjit.impl.common.TestFilters.Multiplier(2),
							new edu.mit.streamjit.impl.common.TestFilters.Adder(20)
						}),
						new edu.mit.streamjit.impl.common.TestFilters.ArrayHasher(1)
					}),
					new Pipeline(new OneToOneElement[]{
						new edu.mit.streamjit.impl.common.TestFilters.Multiplier(3),
						new Pipeline(new OneToOneElement[]{
							new edu.mit.streamjit.impl.common.TestFilters.PeekingAdder(3),
							new edu.mit.streamjit.impl.common.TestFilters.PeekingAdder(3),
							new edu.mit.streamjit.impl.common.TestFilters.ArrayHasher(2),
							new edu.mit.streamjit.impl.common.TestFilters.Adder(20)
						}),
						new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(3), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
							new edu.mit.streamjit.impl.common.TestFilters.ArrayHasher(1),
							new edu.mit.streamjit.impl.common.TestFilters.Multiplier(3)
						}),
						new edu.mit.streamjit.impl.common.TestFilters.Adder(1),
						new edu.mit.streamjit.impl.common.TestFilters.ArrayListHasher(1)
					})
				}),
				new Pipeline(new OneToOneElement[]{
					new edu.mit.streamjit.impl.common.TestFilters.Batcher(2),
					new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(4), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
						new edu.mit.streamjit.impl.common.TestFilters.Adder(1),
						new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(4), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
							new edu.mit.streamjit.impl.common.TestFilters.Adder(1),
							new edu.mit.streamjit.impl.common.TestFilters.ArrayHasher(1),
							new edu.mit.streamjit.api.Identity()
						}),
						new Pipeline(new OneToOneElement[]{
							new edu.mit.streamjit.impl.common.TestFilters.PeekingAdder(10)
						})
					}),
					new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
						new edu.mit.streamjit.impl.common.TestFilters.Adder(1),
						new Splitjoin(new edu.mit.streamjit.api.DuplicateSplitter(), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
							new edu.mit.streamjit.impl.common.TestFilters.Batcher(2)
						})
					}),
					new edu.mit.streamjit.impl.common.TestFilters.PeekingAdder(10)
				})
			}),
			new Pipeline(new OneToOneElement[]{
				new Pipeline(new OneToOneElement[]{
					new edu.mit.streamjit.impl.common.TestFilters.ArrayListHasher(1),
					new Pipeline(new OneToOneElement[]{
						new edu.mit.streamjit.impl.common.TestFilters.Batcher(10),
						new edu.mit.streamjit.impl.common.TestFilters.Adder(20),
						new edu.mit.streamjit.impl.common.TestFilters.ArrayHasher(2),
						new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(4), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
							new edu.mit.streamjit.impl.common.TestFilters.Multiplier(3),
							new edu.mit.streamjit.impl.common.TestFilters.Multiplier(100),
							new edu.mit.streamjit.impl.common.TestFilters.Batcher(2),
							new edu.mit.streamjit.impl.common.TestFilters.Multiplier(3),
							new edu.mit.streamjit.impl.common.TestFilters.Multiplier(2)
						})
					}),
					new Pipeline(new OneToOneElement[]{
						new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(4), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
							new edu.mit.streamjit.impl.common.TestFilters.PeekingAdder(3)
						}),
						new edu.mit.streamjit.api.Identity(),
						new Pipeline(new OneToOneElement[]{
							new edu.mit.streamjit.impl.common.TestFilters.ArrayListHasher(3),
							new edu.mit.streamjit.impl.common.TestFilters.ArrayListHasher(2),
							new edu.mit.streamjit.impl.common.TestFilters.PeekingAdder(10)
						}),
						new edu.mit.streamjit.api.Identity(),
						new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(3), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
							new edu.mit.streamjit.impl.common.TestFilters.ArrayHasher(1)
						})
					}),
					new edu.mit.streamjit.impl.common.TestFilters.ArrayListHasher(3)
				}),
				new edu.mit.streamjit.impl.common.TestFilters.Multiplier(2),
				new Pipeline(new OneToOneElement[]{
					new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(3), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
						new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(2), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
							new edu.mit.streamjit.impl.common.TestFilters.ArrayListHasher(1),
							new edu.mit.streamjit.impl.common.TestFilters.ArrayHasher(1),
							new edu.mit.streamjit.impl.common.TestFilters.Multiplier(100),
							new edu.mit.streamjit.impl.common.TestFilters.Adder(20)
						}),
						new edu.mit.streamjit.impl.common.TestFilters.ArrayHasher(1)
					}),
					new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
						new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(3), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
							new edu.mit.streamjit.impl.common.TestFilters.Batcher(2),
							new edu.mit.streamjit.impl.common.TestFilters.Multiplier(3)
						}),
						new edu.mit.streamjit.impl.common.TestFilters.Adder(1)
					}),
					new Pipeline(new OneToOneElement[]{
						new edu.mit.streamjit.impl.common.TestFilters.ArrayListHasher(1),
						new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(2), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
							new edu.mit.streamjit.impl.common.TestFilters.Batcher(10),
							new edu.mit.streamjit.impl.common.TestFilters.PeekingAdder(3),
							new edu.mit.streamjit.api.Identity()
						}),
						new Pipeline(new OneToOneElement[]{
							new edu.mit.streamjit.impl.common.TestFilters.Multiplier(3)
						}),
						new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(2), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
							new edu.mit.streamjit.impl.common.TestFilters.PeekingAdder(3),
							new edu.mit.streamjit.impl.common.TestFilters.ArrayHasher(1),
							new edu.mit.streamjit.impl.common.TestFilters.ArrayListHasher(1)
						}),
						new edu.mit.streamjit.impl.common.TestFilters.ArrayHasher(3)
					}),
					new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(4), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
						new Pipeline(new OneToOneElement[]{
							new edu.mit.streamjit.impl.common.TestFilters.PeekingAdder(10),
							new edu.mit.streamjit.impl.common.TestFilters.ArrayHasher(3),
							new edu.mit.streamjit.impl.common.TestFilters.PeekingAdder(3)
						})
					}),
					new edu.mit.streamjit.impl.common.TestFilters.ArrayListHasher(2)
				}),
				new edu.mit.streamjit.impl.common.TestFilters.ArrayListHasher(3),
				new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
					new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
						new Pipeline(new OneToOneElement[]{
							new edu.mit.streamjit.impl.common.TestFilters.ArrayListHasher(1)
						})
					})
				})
			}),
			new Pipeline(new OneToOneElement[]{
				new edu.mit.streamjit.impl.common.TestFilters.ArrayHasher(2),
				new edu.mit.streamjit.impl.common.TestFilters.Multiplier(100),
				new edu.mit.streamjit.impl.common.TestFilters.PeekingAdder(10),
				new Splitjoin(new edu.mit.streamjit.api.DuplicateSplitter(), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
					new edu.mit.streamjit.api.Identity(),
					new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(3), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
						new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
							new edu.mit.streamjit.api.Identity()
						})
					}),
					new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(4), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
						new edu.mit.streamjit.impl.common.TestFilters.ArrayHasher(1),
						new edu.mit.streamjit.impl.common.TestFilters.Adder(20),
						new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(4), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
							new edu.mit.streamjit.impl.common.TestFilters.PeekingAdder(10)
						}),
						new edu.mit.streamjit.impl.common.TestFilters.Multiplier(2),
						new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(2), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
							new edu.mit.streamjit.impl.common.TestFilters.Batcher(10)
						})
					}),
					new Splitjoin(new edu.mit.streamjit.api.DuplicateSplitter(), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
						new Pipeline(new OneToOneElement[]{
							new edu.mit.streamjit.impl.common.TestFilters.Adder(20),
							new edu.mit.streamjit.impl.common.TestFilters.Multiplier(2),
							new edu.mit.streamjit.impl.common.TestFilters.Batcher(10),
							new edu.mit.streamjit.impl.common.TestFilters.Multiplier(2),
							new edu.mit.streamjit.impl.common.TestFilters.PeekingAdder(3)
						})
					})
				})
			})
		});
	}
	@Override
	public List<Dataset> inputs() {
		Dataset ds = Datasets.allIntsInRange(0, 1000);
		return Collections.singletonList(ds.withOutput(Datasets.outputOf(new edu.mit.streamjit.impl.interp.InterpreterStreamCompiler(), instantiate(), ds.input())));
	}
	@Override
	public String toString() {
		return getClass().getSimpleName();
	}
	public static void main(String[] args) {
		Benchmarker.runBenchmark(new Reg20131129_095403_915(), new edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler()).get(0).print(System.out);
	}
}

