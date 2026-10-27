#!/usr/bin/env python3
"""Generate clangd metadata from the Makefile's compilation recipes."""

import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile


def generate(make):
    result = subprocess.run(
        [make, "--no-print-directory", "--dry-run", "--always-make", "--jobs=1",
         "COMPILATION_DATABASE=1", "_compile_commands"],
        check=True, capture_output=True, text=True,
    )
    commands = []
    for line in result.stdout.splitlines():
        arguments = shlex.split(line)
        if "-c" not in arguments:
            continue
        source = arguments[arguments.index("-c") + 1]
        output = arguments[arguments.index("-o") + 1]
        commands.append({
            "directory": os.getcwd(),
            "file": source,
            "arguments": arguments,
            "output": output,
        })
    if not commands:
        raise ValueError("Make did not produce any compilation commands")
    commands.sort(key=lambda command: (command["file"], command["output"]))
    contents = json.dumps(commands, indent=2) + "\n"
    destination = Path("compile_commands.json")
    if destination.exists() and destination.read_text() == contents:
        return
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", dir=".", prefix=".compile_commands.",
                                         suffix=".tmp", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(contents)
        temporary.replace(destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


if __name__ == "__main__":
    try:
        generate(sys.argv[1] if len(sys.argv) > 1 else "make")
    except subprocess.CalledProcessError as error:
        sys.exit("Could not generate compile_commands.json:\n" + error.stderr)
    except (OSError, ValueError, IndexError) as error:
        sys.exit("Could not generate compile_commands.json: " + str(error))
