
## Shared tree and disk (read this - incidents happened)
- Other agents edit this tree at the same time. For a mutation check make the smallest possible edit, run, then undo
  exactly that edit (Edit tool, old/new swapped). NEVER save a whole-file copy and restore it over the live file - that
  erased another agent's work once. Before reporting, grep your mutation marker / `if (false &&` and confirm none left.
- The disk is nearly full. Build with `TMPDIR=/dev/shm/<yourname>` and `-j2`; delete that dir at the end. Run test
  executables with gtest filters for what you need rather than full suites in parallel. If you see "No space left on
  device", it is the environment - wait (a capped loop) and retry, do not treat it as a product failure. Never delete
  files you did not create.
