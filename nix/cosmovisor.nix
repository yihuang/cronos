{ buildGoModule, cosmos-sdk }:
buildGoModule rec {
  name = "cosmovisor";
  src = cosmos-sdk + "/cosmovisor";
  subPackages = [ "./cmd/cosmovisor" ];
  vendorSha256 = "sha256-OAXWrwpartjgSP7oeNvDJ7cTR9lyYVNhEM8HUnv3acE=";
  doCheck = false;
}
