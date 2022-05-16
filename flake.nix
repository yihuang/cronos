{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/release-21.11";
    nix-bundle-exe = {
      url = "github:3noch/nix-bundle-exe";
      flake = false;
    };
    gomod2nix = {
      url = "github:tweag/gomod2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
    nix-bundle = {
      url = "github:matthewbauer/nix-bundle";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, nix-bundle-exe, gomod2nix, flake-utils, nix-bundle }:
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
        nix-bundle = import nix-bundle { nixpkgs = final; };
        bundle-arx = drv:
          let
            program = "${drv}/bin/${drv.meta.mainProgram}";
            script = final.writeScript "startup" ''
              #!/bin/sh
              .${final.nix-bundle.nix-user-chroot}/bin/nix-user-chroot -n ./nix -- ${program} "$@"
            '';
            arx = final.nix-bundle.makebootstrap {
              targets = [ script ];
              startup = ".${builtins.unsafeDiscardStringContext script} '\"$@\"'";
            };
          in
          final.runCommand ("arx-" + drv.name) { } ''
            mkdir -p $out/bin
            cp ${arx} $out/bin/
          '';
        make-tarball = drv: final.runCommand drv.name { } ''
          "${final.gnutar}/bin/tar" cfzhv $out -C ${drv} .
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
              "arx" # single-file archive executable using chroot, linux only
              "arxtarball"
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
                  arx-bundle = bundle-arx cronosd;
                in
                if pkgtype == "bundle" then
                  bundle
                else if pkgtype == "tarball" then
                  make-tarball bundle
                else if pkgtype == "arx" then
                  arx-bundle
                else if pkgtype == "arxtarball" then
                  make-tarball arx-bundle
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
