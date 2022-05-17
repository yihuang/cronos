{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/release-21.11";
    nix-bundle-exe = {
      url = "github:yihuang/nix-bundle-exe/delete-empty-directories";
      flake = false;
    };
    gomod2nix = {
      url = "github:tweag/gomod2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, nix-bundle-exe, gomod2nix, flake-utils }:
    let
      rev = self.shortRev or "dirty";
      mkApp = drv: {
        type = "app";
        program = "${drv}/bin/${drv.meta.mainProgram}";
      };
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
          packages = pkgs.cronos-matrix;
          apps = {
            cronosd = mkApp packages.cronosd;
            cronosd-testnet = mkApp packages.cronosd-testnet;
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
        make-tarball = drv: with final; runCommand drv.name { } ''
          "${gnutar}/bin/tar" cfv - -C ${drv} \
            --owner=0 --group=0 --mode=u+rw,uga+r --hard-dereference . \
            | "${gzip}/bin/gzip" -9 > $out
        '';
      } // (with final;
        let
          matrix = lib.cartesianProductOfSets {
            db_backend = [ "goleveldb" "rocksdb" ];
            network = [ "mainnet" "testnet" ];
            pkgtype = [
              "nix" # normal nix package
              "bundle" # relocatable bundled package
              "tarball" # tarball of the bundle, for distribution and checksum
            ];
          };
          binaries = builtins.listToAttrs (builtins.map
            ({ db_backend, network, pkgtype }: {
              name = builtins.concatStringsSep "-" (
                [ "cronosd" ] ++
                lib.optional (network != "mainnet") network ++
                lib.optional (db_backend != "rocksdb") db_backend ++
                lib.optional (pkgtype != "nix") pkgtype
              );
              value =
                let
                  cronosd = callPackage ./. { inherit rev db_backend network; };
                  bundle = bundle-exe cronosd;
                in
                if pkgtype == "bundle" then
                  bundle
                else if pkgtype == "tarball" then
                  make-tarball bundle
                else
                  cronosd;
            })
            matrix
          );
        in
        {
          cronos-matrix = binaries;
        }
      );
    };
}
