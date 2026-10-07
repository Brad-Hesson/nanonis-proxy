{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    crane.url = "github:ipetkov/crane";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    brad-utils.url = "github:Brad-Hesson/brad-utils";
  };
  outputs = flakes: flakes.flake-utils.lib.eachDefaultSystem (system:
    let
      pkgs = import flakes.nixpkgs {
        inherit system;
        config.allowUnfree = true;
      };
      brad-utils = flakes.brad-utils.mkLib pkgs;
      fenix = flakes.fenix.packages.${system};
      crane = (flakes.crane.mkLib pkgs).overrideToolchain (fenix.combine [
        fenix.stable.defaultToolchain
        fenix.stable.rust-src
      ]);
      crateArgs = {
        src = ./.;
        strictDeps = true;
      };
      cargoArtifacts = crane.buildDepsOnly crateArgs;
      crate = crane.buildPackage (crateArgs // {
        inherit cargoArtifacts;
        doCheck = false;
      });
    in
    {
      devShell = crane.devShell {
        inputsFrom = [ crate ];
        shellHook = ''
          ${brad-utils.vscodeSettingsHook {}}
        '';
      };
    }
  );
}

