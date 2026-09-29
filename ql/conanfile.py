from conan import ConanFile
from conan.tools.cmake import CMakeToolchain, CMake, cmake_layout
class RuneQl(ConanFile):
    name = "rune-ql"; version = "0.1.0"; settings = "os","compiler","build_type","arch"
    generators = "CMakeDeps","CMakeToolchain"
    def requirements(self):
        self.test_requires("gtest/1.14.0"); self.test_requires("benchmark/1.8.2")
    def layout(self): cmake_layout(self)
