{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/043f3d6493415fa51ea24d1228bbee82e4aac306";
    flake-utils.url = "github:numtide/flake-utils";
    gomod2nix = {
      url = "github:tweag/gomod2nix/master";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    rocksdb = {
      url = "github:facebook/rocksdb/v6.27.3";
      flake = false;
    };
    pystarport = {
      url = "github:crypto-com/pystarport/main";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    dapptools = {
      # update after merged: https://github.com/dapphub/dapptools/pull/907
      url = "github:yihuang/dapptools/patch-1";
      flake = false;
    };
    cosmos-sdk = {
      url = "github:cosmos/cosmos-sdk/v0.45.0";
      flake = false;
    };
    gravity-bridge = {
      url = "github:crypto-org-chain/gravity-bridge/cronos";
      flake = false;
    };
  };
  outputs = { self, nixpkgs, gomod2nix, flake-utils, rocksdb, pystarport, dapptools, cosmos-sdk, gravity-bridge }:
    (flake-utils.lib.eachDefaultSystem
      (system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [
              self.overlay
              gomod2nix.overlay
            ];
            config = { };
          };
        in
        rec {
          packages = {
            cronosd = pkgs.callPackage ./. { };
            cronosd-testnet = pkgs.callPackage ./. { network = "testnet"; };
          };
          apps = {
            cronosd = {
              type = "app";
              program = "${packages.cronosd}/bin/cronosd";
            };
          };
          defaultPackage = packages.cronosd;
          defaultApp = apps.cronosd;
          devShells = {
            cronosd = pkgs.mkShell {
              buildInputs = with pkgs; [
                go
                rocksdb
                gomod2nix
              ];
            };
            integration-tests = pkgs.callPackage ./nix/test-shell.nix {
              inherit system dapptools cosmos-sdk gravity-bridge;
              cronosd = defaultPackage;
              pystarport = pystarport.defaultPackage.${system};
            };
          };
          devShell = devShells.cronosd;
        }
      )
    ) // {
      overlay = final: prev: {
        go = prev.go_1_17;
        rocksdb = prev.rocksdb.overrideAttrs (_: {
          version = "6.27.3";
          src = rocksdb;
        });
      };
    };
}
