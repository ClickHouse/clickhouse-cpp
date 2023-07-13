from conans import ConanFile, CMake, tools


class ClickHouseCppConan(ConanFile):
    name = "clickhouse-cpp"
    version = "2.5.0"
    homepage = "https://github.com/caaatch22/clickhouse-cpp"
    description = "Modified ClickHouse C++ API, select with raw blob"
    license = "Apache-2.0"
    topics = ("database", "db", "clickhouse")
    settings = "os", "arch", "compiler", "build_type"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        "enable_benchmark": [True, False],
        "with_openssl": [True, False]
    }
    default_options = {
        "shared": False,
        "fPIC": True,
        "enable_benchmark": False,
        "with_openssl": False
    }
    
    generators = "cmake"
    exports_sources = "clickhouse/*"

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC

    def configure(self):
        self.options.rm_safe("fPIC")

    def requirements(self):
        self.requires("lz4/1.9.4")
        self.requires("abseil/20230125.3", transitive_headers=True)

        self.requires("cityhash/cci.20130801")
        if self.options.with_openssl:
            self.requires("openssl/[>=1.1 <4]")

    def build_requirements(self):
        if self.options.enable_benchmark:
            self.requires("benchmark/1.8.0")

    # @property
    # def _min_cppstd(self):
    #     return "17"

    # @property
    # def _compilers_minimum_version(self):
    #     return {
    #         "Visual Studio": "15",
    #         "msvc": "191",
    #         "gcc": "7",
    #         "clang": "6",
    #     }

    # @property
    # def _requires_compiler_rt(self):
    #     return self.settings.compiler == "clang" and (( self.settings.compiler.libcxx in ["libstdc++", "libstdc++11"] and not self.options.shared) or  self.settings.compiler.libcxx == "libc++" )

    # def validate(self):
    #     if self.settings.compiler.get_safe("cppstd"):
    #         check_min_cppstd(self, self._min_cppstd)
    #     minimum_version = self._compilers_minimum_version.get(str(self.settings.compiler), False)
    #     if minimum_version and Version(self.settings.compiler.version) < minimum_version:
    #         raise ConanInvalidConfiguration(f"{self.ref} requires C++17, which your compiler does not support.")
    #     if self.settings.os == "Windows" and self.options.shared:
    #         raise ConanInvalidConfiguration("f{self.ref} does not support shared library on Windows.")
            # look at https://github.com/ClickHouse/clickhouse-cpp/pull/226

    # def layout(self):
    #     cmake_layout(self)

    # def source(self):
    #     get(self, **self.conan_data["src"][self.version],
    #         destination=self.source_folder, strip_root=True)

    def build(self):
        cmake = CMake(self)
        cmake.configure(source_folder="clickhouse")
        cmake.build()

    def package(self):
        self.copy("*.h", dst="include/clickhouse/base", src="clickhouse/base", keep_path=False)
        self.copy("*.h", dst="include/clickhouse/columns", src="clickhouse/columns", keep_path=False)
        self.copy("*.h", dst="include/clickhouse/types", src="clickhouse/types", keep_path=False)
        self.copy("block.h", dst="include/clickhouse", src="clickhouse/", keep_path=False)
        self.copy("client.h", dst="include/clickhouse", src="clickhouse/", keep_path=False)
        self.copy("exceptions.h", dst="include/clickhouse", src="clickhouse/", keep_path=False)
        self.copy("error_codes.h", dst="include/clickhouse", src="clickhouse/", keep_path=False)
        self.copy("protocol.h", dst="include/clickhouse", src="clickhouse/", keep_path=False)
        self.copy("query.h", dst="include/clickhouse", src="clickhouse/", keep_path=False)

        self.copy("server_exception.h", dst="include/clickhouse", src="clickhouse/", keep_path=False)
        self.copy("*.hpp", dst="include/clickhouse", src="clickhouse", keep_path=True)
        self.copy("*.lib", dst="lib", keep_path=False)
        self.copy("*.dll", dst="bin", keep_path=False)
        self.copy("*.so", dst="lib", keep_path=False)
        self.copy("*.dylib", dst="lib", keep_path=False)
        self.copy("*.a", dst="lib", keep_path=False)

    def package_info(self):
        self.cpp_info.libs = ["clickhouse-cpp"]


    # def generate(self):
    #     tc = CMakeToolchain(self)
    #     tc.variables["BUILD_BENCHMARK"] =  self.options.enable_benchmark
    #     tc.cache_variables["BUILD_SHARED_LIBS"] = self.options.shared
    #     tc.variables["WITH_OPENSSL"] = self.options.with_openssl
    #     tc.cache_variables["WITH_SYSTEM_ABSEIL"] = True
    #     tc.cache_variables["WITH_SYSTEM_LZ4"] = True
    #     tc.cache_variables["WITH_SYSTEM_CITYHASH"] = True
    #     tc.generate()

    #     cd = CMakeDeps(self)
    #     cd.generate()


    # def package(self):
    #     copy(self, "LICENSE", src=self.source_folder, dst=os.path.join(self.package_folder, "licenses"))
    #     cmake = CMake(self)
    #     cmake.install()

    # def package_info(self):
    #     self.cpp_info.libs.append("clickhouse-cpp-lib")
    #     self.cpp_info.set_property("cmake_target_name", "clickhouse-cpp-lib::clickhouse-cpp-lib")

    #     if self._requires_compiler_rt:
    #         ldflags = ["--rtlib=compiler-rt"]
    #         self.cpp_info.exelinkflags = ldflags
    #         self.cpp_info.sharedlinkflags = ldflags
    #         self.cpp_info.system_libs.append("gcc_s")

    #     self.cpp_info.filenames["cmake_find_package"] = "clickhouse-cpp"
    #     self.cpp_info.filenames["cmake_find_package_multi"] = "clickhouse-cpp"
    #     self.cpp_info.names["cmake_find_package"] = "clickhouse-cpp-lib"
    #     self.cpp_info.names["cmake_find_package_multi"] = "clickhouse-cpp-lib"

    #     if self.settings.os == 'Windows':
    #         self.cpp_info.system_libs = ['ws2_32', 'wsock32']

