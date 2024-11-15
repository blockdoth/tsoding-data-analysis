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
          shellHook = "
            echo 'Scala lives at ${pkgs.scala_2_12}'
            echo 'SBT lives at ${pkgs.sbt}'
            echo 'java lives at ${pkgs.jdk17}'

          ";
          JAVA_HOME = "${pkgs.jdk17}";
          packages = with pkgs; [
            sbt
            jdk17
          ];
        };
      });
    };
}
