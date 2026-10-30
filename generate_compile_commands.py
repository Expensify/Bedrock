#!/usr/bin/env python3
"""Generate clangd metadata from the Makefile's compilation recipes."""

import json
import os
import shlex
import subprocess
import sys
import tempfile

from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass
class CompileCommand:
    directory: str
    file: str
    arguments: list[str]
    output: str


def makeEnvironment():
    """
    Prepare the environment for a separate Make dry-run.
    Keep Make's evaluation flags and command-line overrides, but drop options specific to the parent run.
    """
    environment = os.environ.copy()
    options, separator, overrides = environment.get('MAKEFLAGS', '').partition(' -- ')
    semanticFlags = ''
    for index, option in enumerate(shlex.split(options)):
        if option.startswith('--') or (index and not option.startswith('-')):
            continue
        for flag in option.lstrip('-'):
            if flag in 'CfIjloOW':
                # These arguments aren't relevant to the dry-run, ignore them
                break
            if flag in 'erR':
                semanticFlags += flag
    # Keep GNU Make's escaped command-line assignments intact, without diagnostic output or jobserver flags.
    environment['MAKEFLAGS'] = semanticFlags + (' -- ' + overrides if separator else '')
    environment.pop('MFLAGS', None)
    environment.pop('GNUMAKEFLAGS', None)
    return environment


def generate(make):
    result = subprocess.run(
        [make, '--no-print-directory', '--dry-run', '--always-make', '--jobs=1',
         'COMPILATION_DATABASE=1', '_compile_commands'],
        check=True, capture_output=True, text=True, env=makeEnvironment(),
    )

    # Parse compile commands
    commands = []
    for line in result.stdout.splitlines():
        arguments = shlex.split(line)
        if '-c' not in arguments:
            continue
        source = arguments[arguments.index('-c') + 1]
        output = arguments[arguments.index('-o') + 1]
        commands.append(CompileCommand(
            directory=os.getcwd(),
            file=source,
            arguments=arguments,
            output=output,
        ))

    if not commands:
        raise ValueError('Make did not produce any compilation commands')
    commands.sort(key=lambda command: (command.file, command.output))
    contents = json.dumps([asdict(command) for command in commands], indent=2) + '\n'
    destination = Path('compile_commands.json')
    if destination.exists() and destination.read_text() == contents:
        return
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', dir='.', prefix='.compile_commands.',
                                         suffix='.tmp', delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(contents)
        temporary.replace(destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


if __name__ == '__main__':
    try:
        generate(sys.argv[1] if len(sys.argv) > 1 else 'make')
    except subprocess.CalledProcessError as error:
        sys.exit('Could not generate compile_commands.json:\n' + error.stderr)
    except (OSError, ValueError, IndexError) as error:
        sys.exit('Could not generate compile_commands.json: ' + str(error))
