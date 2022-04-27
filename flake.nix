{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/release-21.11";
    nix-bundle-exe = {
      url = "github:3noch/nix-bundle-exe";
      flake = false;
    };
    gomod2nix.url = "github:tweag/gomod2nix";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, nix-bundle-exe, gomod2nix, flake-utils }:
    let
      rev = self.rev or "dirty";
    in
    (flake-utils.lib.eachDefaultSystem
      (system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [
              self.overlay
            ];
            config = { };
          };
        in
        rec {
          packages = rec {
            cronosd = pkgs.cronosd;
            cronosd-testnet = pkgs.cronosd-testnet;
            cronosd-exe = pkgs.cronosd-exe;
            cronosd-testnet-exe = pkgs.cronosd-testnet-exe;
          };
          apps = {
            cronosd = {
              type = "app";
              program = "${pkgs.cronosd}/bin/cronosd";
            };
            cronosd-testnet = {
              type = "app";
              program = "${pkgs.cronosd-testnet}/bin/cronosd";
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
          };
          devShell = devShells.cronosd;
        }
      )
    ) // {
      overlay = final: prev: {
        buildGoApplication = final.callPackage (import (gomod2nix + "/builder")) {
          go = final.go_1_17;
        };
        bundle-exe = import nix-bundle-exe { pkgs = final; };
        cronosd = final.callPackage ./. { inherit rev; };
        cronosd-testnet = final.callPackage ./. { inherit rev; network = "testnet"; };
        cronosd-exe = final.bundle-exe final.cronosd;
        cronosd-testnet-exe = final.bundle-exe final.cronosd-testnet;
      };
    };
}
