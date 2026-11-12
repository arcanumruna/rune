#!/usr/bin/env python3
"""The Conan package ships every static library a consumer links.

client-cpp-v0.1.3 failed its test_package link: rune_client_view (ConstraintNames.cpp) was built but neither
installed (RUNE_INSTALL_TARGETS in CMakeLists.txt) nor listed in conanfile.py package_info, so
rune::client::resolveConstraintName was undefined for every consumer.  The release workflow is the only place the
package is linked, so this check catches the drift on every pull request instead:

  * every `add_library(<name> STATIC ...)` in CMakeLists.txt, minus the tool-only libraries in TOOL_ONLY, is in
    RUNE_INSTALL_TARGETS;
  * RUNE_INSTALL_TARGETS and conanfile.py's `client.libs` list the same libraries in the same (dependents-first)
    order.

Usage: check_package_libs.py <client-cpp dir>      exit 0 when consistent
       check_package_libs.py --selftest            the detector catches a missing and a reordered library
"""
import re
import sys
from pathlib import Path

TOOL_ONLY = {"rune_cli_lib"}  # linked by the rune CLI, never by a package consumer


def static_libs(cmake: str) -> set[str]:
    return set(re.findall(r"add_library\(\s*([A-Za-z0-9_]+)\s+STATIC\b", cmake)) - TOOL_ONLY


def install_targets(cmake: str) -> list[str]:
    m = re.search(r"set\(\s*RUNE_INSTALL_TARGETS\s+([^)]*)\)", cmake)
    return m.group(1).split() if m else []


def conan_libs(conanfile: str) -> list[str]:
    m = re.search(r"client\.libs\s*=\s*\[([^\]]*)\]", conanfile)
    return re.findall(r'"([^"]+)"', m.group(1)) if m else []


def problems(cmake: str, conanfile: str) -> list[str]:
    out = []
    installed = install_targets(cmake)
    libs = conan_libs(conanfile)
    if not installed:
        out.append("RUNE_INSTALL_TARGETS not found in CMakeLists.txt")
    if not libs:
        out.append("client.libs not found in conanfile.py")
    for lib in sorted(static_libs(cmake) - set(installed)):
        out.append(f"static library {lib} is built but not in RUNE_INSTALL_TARGETS (a consumer cannot link it)")
    if installed != libs:
        out.append(f"RUNE_INSTALL_TARGETS {installed} != conanfile.py client.libs {libs}")
    return out


def selftest() -> int:
    cmake = ("add_library(a STATIC a.cpp)\nadd_library(b STATIC b.cpp)\nadd_library(rune_cli_lib STATIC c.cpp)\n"
             "set(RUNE_INSTALL_TARGETS\n    a b)\n")
    conan = 'client.libs = [\n    "a", "b",\n]\n'
    cases = [
        ("consistent", cmake, conan, False),
        ("built but not installed", cmake.replace("a b)", "a)"), conan.replace(', "b"', ""), True),
        ("installed but not in conan", cmake, conan.replace(', "b"', ""), True),
        ("order differs", cmake, conan.replace('"a", "b"', '"b", "a"'), True),
    ]
    bad = [name for name, c, k, want in cases if bool(problems(c, k)) != want]
    if bad:
        print("selftest FAILED:", ", ".join(bad))
        return 1
    print("selftest OK")
    return 0


def main() -> int:
    if sys.argv[1:] == ["--selftest"]:
        return selftest()
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    root = Path(sys.argv[1])
    found = problems((root / "CMakeLists.txt").read_text(), (root / "conanfile.py").read_text())
    for p in found:
        print("ERROR:", p)
    if not found:
        print("OK: the package ships every static library a consumer links")
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
