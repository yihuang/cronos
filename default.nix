{ system ? builtins.currentSystem, pkgs ? import ./nix { inherit system; }, db_backend ? "rocksdb" }:
with pkgs;
let
  version = "dev";
  pname = "cronosd";
  tags = lib.concatStringsSep "," (
    [ "mainnet" ]
    ++ lib.lists.optionals (db_backend == "rocksdb") [ "rocksdb" ]
  );
  ldflags = lib.concatStringsSep "\n" ([
    "-X github.com/cosmos/cosmos-sdk/version.Name=cronos"
    "-X github.com/cosmos/cosmos-sdk/version.AppName=${pname}"
    "-X github.com/cosmos/cosmos-sdk/version.Version=${version}"
    "-X github.com/cosmos/cosmos-sdk/version.BuildTags=${tags}"
  ] ++ lib.lists.optionals (db_backend == "rocksdb") [
    "-X github.com/cosmos/cosmos-sdk/types.DBBackend=rocksdb"
  ]);
  buildInputs = lib.lists.optionals (db_backend == "rocksdb") [
    rocksdb-static
  ];
  CGO_LDFLAGS = lib.optionalString (db_backend == "rocksdb") ''
    ${rocksdb-static}/lib/librocksdb.a
    ${bzip2-static.out}/lib/libbz2.a
    ${lz4-static.out}/lib/liblz4.a
    ${zstd-static.out}/lib/libzstd.a
    ${zlib.static}/lib/libz.a
    ${snappy-static.out}/lib/libsnappy.a
  '';
in
buildGoApplication rec {
  inherit pname version buildInputs CGO_LDFLAGS;
  src = (nix-gitignore.gitignoreSourcePure [
    "/*" # ignore all, then add whitelists
    "!/x/"
    "!/app/"
    "!/cmd/"
    "!/client/"
    "!go.mod"
    "!go.sum"
    "!gomod2nix.toml"
  ] ./.);
  modules = ./gomod2nix.toml;
  pwd = src; # needed to support replace
  subPackages = [ "cmd/cronosd" ];
  CGO_ENABLED = "1";
  buildFlags = "-tags=${tags}";
  buildFlagsArray = ''
    -ldflags=
    ${ldflags}
  '';
}
