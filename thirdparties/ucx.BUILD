cc_library(
    name = "ucp",
    srcs = [
        "lib/libucm.so",
        "lib/libucp.so",
        "lib/libucs.so",
        "lib/libucs_signal.so",
        "lib/libuct.so",
    ],
    hdrs = glob([
        "include/ucp/**/*.h",
        "include/ucm/**/*.h",
        "include/ucs/**/*.h",
        "include/uct/**/*.h",
    ]),
    includes = ["include"],
    visibility = ["//visibility:public"],
)

