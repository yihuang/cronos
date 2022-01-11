{ lib, stdenv, fetchFromGitHub, symlinkJoin, openssl, Security, rustPlatform, gravity-bridge }:
rustPlatform.buildRustPackage rec {
  name = "gorc";
  src = gravity-bridge;
  sourceRoot = "source/orchestrator";
  cargoSha256 =
    "sha256:08bpbi7j0jr9mr65hh92gcxys5yqrgyjx6fixjg4v09yyw5im9x7";
  cargoBuildFlags = "-p ${name} --features ethermint";
  buildInputs = lib.optionals stdenv.isDarwin
    [ Security ];
  doCheck = false;
  OPENSSL_NO_VENDOR = "1";
  OPENSSL_DIR = symlinkJoin {
    name = "openssl";
    paths = with openssl; [ out dev ];
  };
}
