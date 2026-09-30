# rune_aeron_util — header-only Aeron helpers shared by consensus/replication/
# gateway/node (spec §3.6 invoker pumping).  INTERFACE: no sources.
add_library(rune_aeron_util INTERFACE)
target_include_directories(rune_aeron_util INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(rune_aeron_util INTERFACE aeron::aeron)
target_compile_features(rune_aeron_util INTERFACE cxx_std_23)
