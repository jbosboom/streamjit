StreamJIT
=========

StreamJIT is an embedded domain-specific language and commensal compiler
for synchronous dataflow stream programming, strongly inspired by
StreamIt.

For more details on StreamJIT and commensal compilation, see [StreamJIT: A
Commensal Compiler for High-Performance Stream Programming](http://groups.csail.mit.edu/commit/papers/2014/bosboom-oopsla14-commensal.pdf),
published in OOPSLA 2014.  The [talk slides are also available](http://groups.csail.mit.edu/commit/papers/2014/bosboom-oopsla14-commensal-slides.pdf).

Building
--------

`ant fetch; ant jar; ant test`

Using
-----

The current best documentation is the getting started guide prepared for 
the OOPSLA artifact submission, in `doc/getting-started-guide.pdf`.
Further questions can be directed to the authors.

Note that the distributed backend in this repository is out-of-date due to
a long-running branch.  Contact Sumanan (e-mail address listed in the paper)
if you're interested in that.
