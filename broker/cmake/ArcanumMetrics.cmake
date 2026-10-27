# ── arcanum-metrics — header-only mmap metrics file (Aeron CnC-style counters) ──
# Consumed exactly like the flight recorder (plan D60): it lives in its own repo
# (github.com/arcanumruna/arcanum-metrics).  -DARCANUM_METRICS_DIR=<checkout with include/>
# overrides it (local development, docker builds pass /arcanum-metrics); otherwise
# FetchContent clones it at the pinned commit (clone only: the library is header-only,
# so no subproject build is added).  The conan package is not used.
#
# Defines the INTERFACE target `metrics` (include/ + C++23).  Included by
# broker/CMakeLists.txt and by the configure test in broker/tests/cmake/.
set(ARCANUM_METRICS_DIR "" CACHE PATH "Local arcanum-metrics checkout; empty = fetch it")
set(ARCANUM_METRICS_GIT_REPOSITORY "git@github.com:arcanumruna/arcanum-metrics.git" CACHE STRING
    "arcanum-metrics git URL used when ARCANUM_METRICS_DIR is unset")
set(ARCANUM_METRICS_GIT_TAG "991ecc56ce2f7a02c6dd9f613cb2514e7849206e" CACHE STRING
    "pinned arcanum-metrics commit (a sha, never a branch)")
if(NOT ARCANUM_METRICS_DIR)
    include(FetchContent)
    FetchContent_Declare(arcanum_metrics_src
        GIT_REPOSITORY ${ARCANUM_METRICS_GIT_REPOSITORY}
        GIT_TAG        ${ARCANUM_METRICS_GIT_TAG}
        GIT_SHALLOW    FALSE
        SOURCE_SUBDIR  _no_cmake_build)
    FetchContent_MakeAvailable(arcanum_metrics_src)
    set(ARCANUM_METRICS_DIR "${arcanum_metrics_src_SOURCE_DIR}" CACHE PATH "" FORCE)
endif()
if(NOT EXISTS "${ARCANUM_METRICS_DIR}/include/metrics/MetricsLayout.h")
    message(FATAL_ERROR "ARCANUM_METRICS_DIR (${ARCANUM_METRICS_DIR}) is not an arcanum-metrics checkout "
                        "(no include/metrics/MetricsLayout.h)")
endif()
if(NOT TARGET metrics)
    add_library(metrics INTERFACE)
    target_include_directories(metrics INTERFACE ${ARCANUM_METRICS_DIR}/include)
    target_compile_features(metrics INTERFACE cxx_std_23)
endif()
