{
  description = "pgxcursor — cursor-based iterator for pgx";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    devcontainer-env.url = "github:devcontainer-env/devcontainer-env";
  };

  outputs =
    {
      nixpkgs,
      flake-utils,
      devcontainer-env,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        devShells.default = pkgs.mkShell {
          name = "pgxcursor";
          packages = [
            devcontainer-env.packages.${system}.default
            pkgs.go
          ];
        };
      }
    );
}
