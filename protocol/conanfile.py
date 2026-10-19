from conan import ConanFile
from conan.tools.cmake import CMakeToolchain, CMakeDeps, CMake, cmake_layout
import os

class RuneProtocolConan(ConanFile):
    name = "rune-protocol"
    package_type = "library"
    settings = "os", "compiler", "build_type", "arch"
    options = {"shared": [True, False]}
    default_options = {"shared": False}
    exports_sources = "src/*", "include/*", "CMakeLists.txt", "VERSION.txt"

    def set_version(self):
        self.version = open(os.path.join(os.path.dirname(__file__), "VERSION.txt")).read().strip()

    def requirements(self):
        for r in self.conan_data.get("requirements", []):
            self.requires(r)

    def build_requirements(self):
        for r in self.conan_data.get("test_requirements", []):
            self.test_requires(r)
        for r in self.conan_data.get("benchmark_requirements", []):
            self.test_requires(r)

    def generate(self):
        CMakeDeps(self).generate()
        CMakeToolchain(self).generate()

    def layout(self):
        cmake_layout(self)

    def build(self):
        cmake = CMake(self)
        cmake.configure()

    def package(self):
        CMake(self).install()

    def package_info(self):
        self.cpp_info.set_property("cmake_target_name", "rune-protocol")
        self.cpp_info.includedirs = ["include"]
