#!/bin/bash

set -u

wsm="./target/wasm32-wasip1/release-wasi/fs-stats2ipc4du.wasm"

iproto="./sample.d/input.protobuf.maps.dat"

geninput(){
  echo creating input file...

  mkdir -p ./sample.d

  jmaps2pmaps="${HOME}/.cargo/bin/jsonmaps2protomaps.wasm"

  test -f "${jmaps2pmaps}" || exec sh -c '
    echo helper wasi jsonmaps2protomaps.wasm not found.
    exit 1
  '

  jq -c -n '[
    {
      filename:"./dummy",
      dev: 16777233,
      ino: 125008195,
      mode: 4516,
      nlink: 1,
      uid: 0,
      gid: 0,
      rdev: 0,
      size: 1234,
      atime: 1789942957,
      mtime: 1789942957,
      ctime: 1789942957,
      blksize: 5678,
      blocks: 42,
      flags: 0,
      gen: 0,
    },
    {
      filename:"./NOT_FOUND",
      not_found: true,
      raw_os_error: 2,
    }
  ]' |
    jq -c '.[]' |
    wasmtime run "${jmaps2pmaps}" |
    cat > "${iproto}"
}

test -f "${iproto}" || geninput

cat "${iproto}" |
  wasmtime run "${wsm}" |
  arrow-cat
