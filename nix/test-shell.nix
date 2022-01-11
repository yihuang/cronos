{ fetchFromGitHub
, callPackage
, mkShell
, lib
, poetry2nix
, darwin
, buildGo117Module
, system
, jq
, poetry
, yarn
, nodejs
, nixpkgs-fmt
, cronosd
, pystarport
, dapptools
, cosmos-sdk
, gravity-bridge
}:
let
  dapptools-pkgs = {
    x86_64-linux =
      (import (dapptools + "/release.nix") { }).dapphub.linux.stable;
    x86_64-darwin =
      (import (dapptools + "/release.nix") { }).dapphub.darwin.stable;
  }.${system} or (throw
    "Unsupported system: ${system}");
  # use a version that supports eip-1559
  go-ethereum = callPackage ../nix/go-ethereum.nix {
    inherit (darwin) libobjc;
    inherit (darwin.apple_sdk.frameworks) IOKit;
    buildGoModule = buildGo117Module;
  };
  chainmain = import ../nix/chainmain.nix { inherit system; };
  hermes = callPackage ../nix/hermes.nix { };
  gorc = callPackage ../nix/gorc.nix {
    inherit gravity-bridge;
    inherit (darwin.apple_sdk.frameworks) Security;
  };
  cosmovisor = callPackage ../nix/cosmovisor.nix {
    inherit cosmos-sdk;
    buildGoModule = buildGo117Module;
  };
  test-env = poetry2nix.mkPoetryEnv
    {
      projectDir = ../integration_tests;
      overrides = poetry2nix.overrides.withDefaults (self: super: {
        eth-bloom = super.eth-bloom.overridePythonAttrs {
          preConfigure = ''
            substituteInPlace setup.py --replace \'setuptools-markdown\' ""
          '';
        };
      });
    };
  scripts = callPackage ../nix/scripts.nix {
    inherit pystarport chainmain go-ethereum hermes;
    config = {
      chainmain-config = ../scripts/chainmain-devnet.yaml;
      cronos-config = ../scripts/cronos-devnet.yaml;
      hermes-config = ../scripts/hermes.toml;
      geth-genesis = ../scripts/geth-genesis.json;
      dotenv = builtins.path { name = "dotenv"; path = ../scripts/.env; };
    };
  };
in
mkShell {
  propagatedBuildInputs = [
    cronosd
  ];
  buildInputs = [
    jq
    poetry
    yarn
    nodejs
    dapptools-pkgs.dapp
    dapptools-pkgs.solc-versions.solc_0_6_8
    go-ethereum
    pystarport
    gorc
    cosmovisor
    nixpkgs-fmt
    chainmain
    hermes
    test-env
    scripts.start-scripts
  ];
}
