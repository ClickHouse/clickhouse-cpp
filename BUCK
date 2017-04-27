cxx_library(
  name = 'clickhouse-cpp',
  header_namespace = 'clickhouse',
  exported_headers = subdir_glob([
    ('clickhouse', '**/*.h'),
  ]),
  srcs = glob([
    'clickhouse/**/*.cpp',
  ]),
  compiler_flags = [
    '-std=c++11',
  ],
  visibility = [
    'PUBLIC',
  ],
  deps = [
    '//contrib/cityhash:cityhash',
    '//contrib/lz4:lz4',
  ]
)
