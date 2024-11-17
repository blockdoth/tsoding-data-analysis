{
  description = "A Nix flake template for a scala development environment";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      supportedSystems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forEachSupportedSystem = f: nixpkgs.lib.genAttrs supportedSystems (system: f {
        pkgs = import nixpkgs { inherit system; };
      });
    in
    {
      devShells = forEachSupportedSystem ({ pkgs }: {
        default = pkgs.mkShell {
          JAVA_HOME = "${pkgs.jdk8}";
          packages = with pkgs; [
            sbt
            scala_2_11
            maven
            jdk8
            python3
            python3Packages.matplotlib
            python3Packages.numpy
            python3Packages.jupyterlab
          ];
        };
      });
    };
}
