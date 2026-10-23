from conan import ConanFile
from conan.errors import ConanException, ConanInvalidConfiguration
from conan.tools.cmake import cmake_layout, CMake, CMakeToolchain, CMakeDeps
from conan.tools.files import copy
import os

# The sibling source directories the build compiles (see CMakeLists.txt); they ship inside the recipe.
# cmake/ holds the per-process test temp root (issue #375) the test build links into every gtest binary.
SIBLINGS = ("protocol", "codec", "crypto", "ql", "cmake")
# Never exported: build trees, tool caches and VCS metadata.
EXCLUDES = ["build*/*", "cmake-build*/*", ".cache/*", ".git/*", "Testing/*", "CMakeUserPresets.json", "compile_commands.json"]


class RuneClientCppConan(ConanFile):
    name = "rune-client-cpp"
    package_type = "static-library"
    settings = "os", "compiler", "build_type", "arch"
    options = {"fPIC": [True, False]}
    default_options = {"fPIC": True}
    generators = "CMakeDeps"

    def set_version(self):
        self.version = open(os.path.join(os.path.dirname(__file__), "VERSION.txt")).read().strip()

    # True when the recipe sits next to its CMakeLists.txt (development: building from client-cpp/ or
    # `conan install .`).  In the Conan cache the recipe is exported without them: sources live in the
    # export-sources folder, laid out as client-cpp/ plus its siblings.
    def _in_source_tree(self):
        return os.path.isfile(os.path.join(self.recipe_folder, "CMakeLists.txt"))

    def _flight_recorder_dir(self):
        # Same lookup as the build: FLIGHT_RECORDER_DIR (a flight-recorder checkout with src/ + include/).
        # Without it the CMake build clones the pinned commit, which a packaged recipe cannot rely on.
        return os.environ.get("FLIGHT_RECORDER_DIR", "")

    def export_sources(self):
        root = os.path.join(self.recipe_folder, "..")
        copy(self, "LICENSE", root, self.export_sources_folder)
        copy(self, "*", self.recipe_folder, os.path.join(self.export_sources_folder, "client-cpp"), excludes=EXCLUDES)
        for sibling in SIBLINGS:
            copy(self, "*", os.path.join(root, sibling), os.path.join(self.export_sources_folder, sibling),
                 excludes=EXCLUDES)
        fr = self._flight_recorder_dir()
        if not os.path.isfile(os.path.join(fr, "src", "Calibration.cpp")):
            raise ConanException("set FLIGHT_RECORDER_DIR to a flight-recorder checkout (src/ + include/) "
                                 "to export rune-client-cpp")
        copy(self, "src/*", fr, os.path.join(self.export_sources_folder, "flight-recorder"))
        copy(self, "include/*", fr, os.path.join(self.export_sources_folder, "flight-recorder"))

    def config_options(self):
        if self.settings.os == "Windows":
            self.options.rm_safe("fPIC")

    def layout(self):
        if self._in_source_tree():
            cmake_layout(self)
        else:
            cmake_layout(self, src_folder="client-cpp")

    def _skip_aeron(self):
        # The aeron/1.41.4 ConanCenter recipe raises ConanInvalidConfiguration on
        # macOS/armv8 (Apple Silicon), breaking `conan install` and the CMake
        # configure in CLion on a Mac.  The client is built/run on Linux; on macOS
        # the project is loaded for editing/indexing only, so skip aeron there
        # (CMakeLists provides a stub aeron::aeron INTERFACE target when absent).
        return str(self.settings.os) == "Macos" and str(self.settings.arch) == "armv8"

    def validate(self):
        # A packaged build must never contain the aeron stub.  The development tree (`conan install .`) may.
        if self._skip_aeron() and not self._in_source_tree():
            raise ConanInvalidConfiguration("rune-client-cpp cannot be packaged on macOS/armv8: aeron does not "
                                            "build there and the IDE stub is not a usable library. Build on Linux.")

    def requirements(self):
        for r in self.conan_data.get("requirements", []):
            if self._skip_aeron() and r.startswith("aeron/"):
                continue
            # The public headers include aeron, spdlog and OpenSSL types: consumers need their headers and libs.
            self.requires(r, transitive_headers=True, transitive_libs=True)

    def build_requirements(self):
        if not self.conf.get("tools.build:skip_test", default=False):
            for r in self.conan_data.get("test_requirements", []):
                self.test_requires(r)

    def generate(self):
        tc = CMakeToolchain(self)
        tc.user_presets_path = False
        if not self._in_source_tree():
            # the exported flight-recorder sits next to client-cpp/ in the export-sources folder
            tc.cache_variables["FLIGHT_RECORDER_DIR"] = os.path.join(self.source_folder, "..", "flight-recorder").replace("\\", "/")
        tc.generate()

    def build(self):
        skip_test = self.conf.get("tools.build:skip_test", default=False)
        cmake = CMake(self)
        cmake.configure(variables={"BUILD_TESTING": "OFF" if skip_test else "ON"})
        cmake.build()
        if not skip_test:
            # the checkpoint suite: everything named *Regression* except the latency gates (idle host only)
            cmake.ctest(cli_args=["-R", "Regression", "-LE", "latency", "--output-on-failure"])

    def package(self):
        copy(self, "LICENSE", os.path.join(self.source_folder, ".."), os.path.join(self.package_folder, "licenses"))
        CMake(self).install()

    def package_info(self):
        # Link order matters for static archives: dependents before their dependencies.
        self.cpp_info.libs = [
            "rune_client_api", "rune_client_producer", "rune_client_subscriber", "rune_client_group",
            "rune_client_agent", "rune_client_transport", "rune_gso", "rune_client_driver_wake",
            "rune_ql", "rune_security", "rune_catalog", "rune_codec",
            "rune_client_scram", "rune_crypto", "flight_recorder",
        ]
        self.cpp_info.defines = ["FLIGHT_RECORDER=1"]
        if self.settings.os in ("Linux", "FreeBSD"):
            self.cpp_info.system_libs = ["uuid", "pthread", "dl", "m"]
        # find_package(rune-client-cpp) -> target rune::client
        self.cpp_info.set_property("cmake_file_name", "rune-client-cpp")
        self.cpp_info.set_property("cmake_target_name", "rune::client")
