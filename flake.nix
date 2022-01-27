{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/release-21.11";
    gomod2nix.url = "github:tweag/gomod2nix/master";
    gomod2nix.inputs.nixpkgs.follows = "nixpkgs";
  };
  outputs = { self, nixpkgs, gomod2nix }: { };
}
