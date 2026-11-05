#!/usr/bin/env bash
# hang-watch.sh - dump the threads of every test process that runs longer than a limit.
#
# A test that hangs in CI is killed at its ctest timeout with nothing past its last log line: no stack, no hint of the
# wait that never ended.  This watcher runs beside ctest and, once per process, attaches gdb to every gtest process
# (a `--gtest_filter=` command line) older than LIMIT seconds and writes `thread apply all bt` to OUT_DIR/hang-<pid>.txt.
# The test keeps running (gdb detaches); ctest still times it out.  Print the dumps after ctest with
#   hang-watch.sh --print OUT_DIR
#
# usage: hang-watch.sh LIMIT_SECONDS OUT_DIR &      (runs until killed)
#        hang-watch.sh --print OUT_DIR              (prints every dump, exit 0)
set -u

if [ "${1:-}" = "--print" ]; then
    shopt -s nullglob
    for f in "${2:?OUT_DIR}"/hang-*.txt; do
        echo "::group::hang dump ${f##*/}"
        cat "$f"
        echo "::endgroup::"
    done
    exit 0
fi

limit=${1:?LIMIT_SECONDS}
out=${2:?OUT_DIR}
mkdir -p "$out"
while true; do
    # pid, elapsed seconds, full command line of every process.
    while read -r pid secs args; do
        case "$args" in *--gtest_filter=*) ;; *) continue ;; esac
        [ "$secs" -ge "$limit" ] || continue
        f="$out/hang-$pid.txt"
        [ -e "$f" ] && continue
        {
            echo "=== hang-watch: pid $pid running ${secs}s: $args"
            sudo -n gdb -batch -nx -p "$pid" -ex 'set pagination off' -ex 'thread apply all bt' 2>&1 \
                || echo "(gdb failed)"
        } > "$f"
        echo "hang-watch: dumped pid $pid (${secs}s) to $f"
    done < <(ps -eo pid=,etimes=,args= 2>/dev/null)
    sleep 10
done
