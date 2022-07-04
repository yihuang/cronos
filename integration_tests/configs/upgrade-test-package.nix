let
  pkgs = import ../../nix { };
  released = (pkgs.flake-compat {
    src = builtins.fetchTarball "https://github.com/crypto-org-chain/cronos/archive/v0.7.0.tar.gz";
  }).defaultNix.default;
  current = pkgs.callPackage ../../. { };
in
pkgs.linkFarm "upgrade-test-package" [
  { name = "genesis"; path = released; }
  { name = "v0.8.0"; path = current; }
]
