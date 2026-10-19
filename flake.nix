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
        nativeBuildInputs = [ pkgs.pkg-config pkgs.makeWrapper ];
        buildInputs = with pkgs; [ libGL libxkbcommon wayland xorg.libX11 xorg.libXcursor xorg.libXi xorg.libXrandr ];
        postFixup = ''
          wrapProgram $out/bin/nanonis-proxy --prefix LD_LIBRARY_PATH : ${pkgs.lib.makeLibraryPath (with pkgs; [ libGL libxkbcommon wayland xorg.libX11 xorg.libXcursor xorg.libXi xorg.libXrandr ])}
        '';
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
        LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath crateArgs.buildInputs;
        shellHook = ''
          ${brad-utils.vscodeSettingsHook {}}
        '';
      };
    }
  );
}

