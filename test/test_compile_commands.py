#!/usr/bin/env python3
"""Exercise compilation metadata in an isolated tree without compiling Bedrock."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import unittest


class CompilationDatabaseTest(unittest.TestCase):
    sources = [
        "main.cpp", "BedrockServer.cpp", "plugins/MySQL.cpp", "plugins/Cache.cpp",
        "libstuff/Stuff.cpp", "libstuff/JSON/Metrics.cpp", "libstuff/JSON/Value.cpp",
        "libstuff/sqlite3.c", "libstuff/qrf.c", "test/main.cpp", "test/tests/Unit.cpp",
        "test/tests/jobs/JobTestHelper.cpp", "test/lib/tpunit++.cpp",
        "test/clustertest/Cluster.cpp", "test/clustertest/testplugin/TestPlugin.cpp",
        "test/clustertest/testplugin/ExternPointer.cpp", "test/portserver/main.cpp",
        "test/portserver/PortServer.cpp", "benchmarks/Bench.cpp",
    ]

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        repository = Path(__file__).resolve().parents[1]
        for filename in ("Makefile", "generate_compile_commands.py"):
            shutil.copyfile(repository / filename, self.root / filename)
        for source in self.sources + ["test/benchmarks/Excluded.cpp"]:
            path = self.root / source
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()
            os.utime(path, (time.time() - 60, time.time() - 60))
        self.compiler = self.root / "fake-compiler"
        self.compiler.write_text('''#!/usr/bin/env python3
import os
from pathlib import Path
import sys
assert Path("compile_commands.json").is_file(), "metadata must precede compilation"
Path("compiler-ran").touch()
if os.environ.get("FAIL_COMPILATION"):
    sys.exit(42)
args = sys.argv[1:]
output = Path(args[args.index("-o") + 1])
output.parent.mkdir(parents=True, exist_ok=True)
output.touch()
if "-MF" in args:
    Path(args[args.index("-MF") + 1]).write_text(str(output) + ":\\n")
''')
        self.compiler.chmod(0o755)
        self.environment = os.environ.copy()
        # Do not inherit the command-line options of a make that launches this test.
        for variable in ("MAKEFLAGS", "MFLAGS", "MAKELEVEL"):
            self.environment.pop(variable, None)

    def command(self, *arguments):
        return ["make", "--no-print-directory", "PROJECT=" + str(self.root),
                "GIT_REVISION=-DGIT_REVISION=fixture", "CXX=" + str(self.compiler),
                "CC=" + str(self.compiler), *arguments]

    def make(self, *arguments, success=True):
        result = subprocess.run(self.command(*arguments), cwd=self.root,
                                env=self.environment, capture_output=True, text=True)
        if success:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0)
        return result

    def database(self):
        return {entry["file"]: entry for entry in
                json.loads((self.root / "compile_commands.json").read_text())}

    def populate_build(self):
        for source in self.sources:
            stem = ".build/" + str(Path(source).with_suffix(""))
            for suffix in (".o", ".d"):
                path = self.root / (stem + suffix)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("")
        for filename in ("bedrock", "bedrock-test-port-server", "libstuff.a", "libjson.a",
                         "libbedrock.a", "test/test", "test/clustertest/clustertest",
                         "test/clustertest/testplugin/testplugin.so",
                         "mbedtls/library/libmbedcrypto.a"):
            path = self.root / filename
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()

    def test_explicit_target_uses_real_recipe_variants_without_building(self):
        self.make("-j8", "compile_commands", "BEDROCK_OPTIM_COMPILE_FLAG=-O0")
        commands = self.database()
        self.assertEqual(set(commands), set(self.sources))
        for source, command in commands.items():
            arguments = command["arguments"]
            self.assertEqual(command["directory"], str(self.root))
            self.assertEqual(arguments[0], str(self.compiler))
            self.assertIn("-O0", arguments)
            if source.endswith(".c"):
                self.assertIn("-std=gnu23", arguments)
                self.assertIn("-DSQLITE_ENABLE_FTS5", arguments)
                self.assertNotIn("-I" + str(self.root), arguments)
            else:
                self.assertIn("-std=c++20", arguments)
                self.assertIn("-Werror", arguments)
                self.assertIn(str(self.root / "externalLib/rapidjson/include"), arguments)
            self.assertEqual("-DGIT_REVISION=fixture" in arguments,
                             source in ("main.cpp", "BedrockServer.cpp", "plugins/MySQL.cpp"))
        self.assertIn("-DTPUNITPP_ENABLE_NCURSES_OUTPUT", commands["test/tests/Unit.cpp"]["arguments"])
        self.assertNotIn("-DTPUNITPP_ENABLE_NCURSES_OUTPUT", commands["benchmarks/Bench.cpp"]["arguments"])
        self.assertFalse((self.root / "compiler-ran").exists())
        self.assertFalse((self.root / ".build").exists())
        self.assertFalse((self.root / "mbedtls").exists())

    def test_command_line_flags_and_quoting_are_preserved(self):
        self.make("compile_commands", "CXXFLAGS=-std=c++17 -DOPTION='quoted value'")
        arguments = self.database()["test/tests/Unit.cpp"]["arguments"]
        self.assertIn("-DOPTION=quoted value", arguments)
        self.assertIn("-std=c++17", arguments)
        self.assertNotIn("-std=c++20", arguments)
        self.assertNotIn("-DTPUNITPP_ENABLE_NCURSES_OUTPUT", arguments)

    def test_refresh_detects_source_changes_and_preserves_unchanged_file(self):
        self.make("compile_commands")
        output = self.root / "compile_commands.json"
        modified = output.stat().st_mtime_ns
        self.make("compile_commands")
        self.assertEqual(output.stat().st_mtime_ns, modified)
        source = self.root / "plugins/Added.cpp"
        source.touch()
        self.make("compile_commands")
        self.assertIn("plugins/Added.cpp", self.database())
        source.unlink()
        self.make("compile_commands")
        self.assertNotIn("plugins/Added.cpp", self.database())

    def test_make_diagnostics_preserve_metadata_and_command_line_overrides(self):
        self.populate_build()
        for option in ('-p', '--debug=a'):
            for target in ('compile_commands', 'bedrock'):
                with self.subTest(option=option, target=target):
                    self.make(option, target, 'CXXFLAGS=-std=c++17 -DOPTION=\'quoted value\'')
                    commands = self.database()
                    self.assertEqual(set(commands), set(self.sources))
                    self.assertIn('-DOPTION=quoted value', commands['main.cpp']['arguments'])
                    self.assertIn('-std=c++17', commands['main.cpp']['arguments'])
                    (self.root / 'compile_commands.json').unlink()
        self.assertFalse((self.root / 'compiler-ran').exists())

    def test_environment_override_precedence_survives_diagnostic_flags(self):
        self.environment['CXXFLAGS'] = '-std=c++17 -DENVIRONMENT=1'
        self.make('-e', '-p', 'compile_commands')
        arguments = self.database()['main.cpp']['arguments']
        self.assertIn('-DENVIRONMENT=1', arguments)
        self.assertNotIn('-std=c++20', arguments)

    def test_generation_failure_preserves_previous_database(self):
        self.make("compile_commands")
        output = self.root / "compile_commands.json"
        previous = output.read_bytes()
        (self.root / "test/clustertest/testplugin/TestPlugin.cpp").unlink()
        self.make("compile_commands", success=False)
        self.assertEqual(output.read_bytes(), previous)
        self.assertFalse(list(self.root.glob(".compile_commands.*.tmp")))

    def test_no_op_bedrock_build_refreshes_without_compiling_or_relinking(self):
        self.populate_build()
        binary = self.root / "bedrock"
        modified = binary.stat().st_mtime_ns
        self.make("bedrock")
        self.assertEqual(set(self.database()), set(self.sources))
        self.assertFalse((self.root / "compiler-ran").exists())
        self.assertEqual(binary.stat().st_mtime_ns, modified)
        (self.root / "compile_commands.json").unlink()
        self.make("bedrock")
        self.assertTrue((self.root / "compile_commands.json").exists())

    def test_metadata_precedes_failed_dependency_compilation(self):
        self.populate_build()
        (self.root / ".build/main.d").unlink()
        self.environment["FAIL_COMPILATION"] = "1"
        self.make("bedrock", success=False)
        self.assertTrue((self.root / "compiler-ran").exists())
        self.assertEqual(set(self.database()), set(self.sources))

    def test_dry_run_query_touch_and_clean_skip_automatic_generation(self):
        self.populate_build()
        for flag in ("-n", "-q", "-t"):
            with self.subTest(flag=flag):
                self.make(flag, "bedrock")
                self.assertFalse((self.root / "compile_commands.json").exists())
        self.make("clean")
        self.assertFalse((self.root / "compile_commands.json").exists())

    def test_concurrent_generators_publish_complete_json(self):
        processes = [subprocess.Popen(self.command("-j4", "compile_commands"),
                                     cwd=self.root, env=self.environment,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                     for _ in range(2)]
        for process in processes:
            stdout, stderr = process.communicate(timeout=30)
            self.assertEqual(process.returncode, 0, stdout + stderr)
        self.assertEqual(set(self.database()), set(self.sources))
        self.assertFalse(list(self.root.glob(".compile_commands.*.tmp")))


if __name__ == "__main__":
    unittest.main()
