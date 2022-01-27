{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/release-21.11";
    flake-utils.url = "github:numtide/flake-utils";
    gomod2nix = {
      url = "github:tweag/gomod2nix/master";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    rocksdb = {
      url = "github:facebook/rocksdb/v6.27.3";
      flake = false;
    };
  };
  outputs = { self, nixpkgs, gomod2nix, flake-utils, rocksdb }:
    (flake-utils.lib.eachDefaultSystem (system:
      let pkgs = (import nixpkgs {
        inherit system;
        overlays = [
          self.overlay
          gomod2nix.overlay
        ];
        config = { };
      });
      in
      {
        defaultPackage = pkgs.callPackage ./. { };
      }
    )) //
    {
      overlay = final: prev: {
        go = prev.go_1_17;
        rocksdb = prev.rocksdb.overrideAttrs (_: {
          version = "6.27.3";
          src = rocksdb;
        });
      };
    };
}
