COPTS = [
    "-DGFLAGS=gflags",
    "-DOS_LINUX",
    "-DSNAPPY",
    "-DHAVE_SSE42",
    "-DNDEBUG",
    "-fno-omit-frame-pointer",
    "-momit-leaf-frame-pointer",
    "-msse4.2",
    "-pthread",
    "-Wsign-compare",
    "-Wno-unused-parameter",
    "-Wno-unused-variable",
    "-Woverloaded-virtual",
    "-Wnon-virtual-dtor",
    "-Wno-missing-field-initializers",
    "-std=c++11",
]

cc_library(
    name = "chunk",
    srcs = glob([
        "*.cpp",
    ]),
    hdrs = glob([
        "*.h",
    ]),
    deps = [
        "//external:gflags",
        "//external:glog",
        "//external:brpc",
        "//external:protobuf",
        "//src/common:curve_common",
        "//src/common:curve_auth",
        "//include/client:include_client",
        "//include:include-common",
        "//proto:nameserver2_cc_proto",
        "//proto:topology_cc_proto",
        "//proto:chunkserver-cc-protos",
    ],
    copts = COPTS,
    visibility = ["//visibility:public"],
)
