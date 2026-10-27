# Configure-only fixture for BrokerMetricsRegression.ConfigureRejectsMissingDirRegression:
# includes the real ArcanumMetrics.cmake so a bad -DARCANUM_METRICS_DIR fails with a clear message.
cmake_minimum_required(VERSION 3.25)
project(arcanum_metrics_dir_check LANGUAGES NONE)
include(${CMAKE_CURRENT_LIST_DIR}/../../../cmake/ArcanumMetrics.cmake)
