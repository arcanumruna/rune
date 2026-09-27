from conan import ConanFile
from conan.tools.cmake import cmake_layout, CMakeToolchain, CMakeDeps
import os

class RuneClientCppConan(ConanFile):
    name = "rune-client-cpp"
    package_type = "application"
    settings = "os", "compiler", "build_type", "arch"
    generators = "CMakeDeps"

    def set_version(self):
        self.version = open(os.path.join(os.path.dirname(__file__), "VERSION.txt")).read().strip()

    def layout(self):
        cmake_layout(self)

    def generate(self):
        tc = CMakeToolchain(self); tc.user_presets_path = False; tc.generate()

    def requirements(self):
        # The aeron/1.41.4 ConanCenter recipe raises ConanInvalidConfiguration on
        # macOS/armv8 (Apple Silicon), breaking `conan install` and the CMake
        # configure in CLion on a Mac.  The client is built/run on Linux; on macOS
        # the project is loaded for editing/indexing only, so skip aeron there
        # (CMakeLists provides a stub aeron::aeron INTERFACE target when absent).
        skip_aeron = (str(self.settings.os) == "Macos" and
                      str(self.settings.arch) == "armv8")
        for r in self.conan_data.get("requirements", []):
            if skip_aeron and r.startswith("aeron/"):
                continue
            self.requires(r)
