from conan import ConanFile
from conan.tools.cmake import cmake_layout, CMakeToolchain, CMakeDeps
import os

class RuneBrokerConan(ConanFile):
    name = "rune-broker"
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
        for r in self.conan_data.get("requirements", []):
            self.requires(r)
