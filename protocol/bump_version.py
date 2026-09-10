#!/usr/bin/env python3
import sys
import re

def bump_version(part):
    with open("VERSION.txt", "r+") as f:
        version = f.read().strip()
        major, minor, patch = map(int, version.split("."))

        if part == "patch":
            patch += 1
        elif part == "minor":
            minor += 1
            patch = 0
        elif part == "major":
            major += 1
            minor = patch = 0
        else:
            print("Usage: bump_version.py [major|minor|patch]")
            sys.exit(1)

        new_version = f"{major}.{minor}.{patch}"
        f.seek(0)
        f.write(new_version)
        f.truncate()

        print(f"Bumped version: {version} → {new_version}")

if __name__ == "__main__":
    bump_version(sys.argv[1] if len(sys.argv) > 1 else "patch")
