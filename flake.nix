{
  description = "Python 3.12 development environment with Rust/PyO3 support";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
        };

        # 1. Common Libraries
        buildLibraries = with pkgs; [
          stdenv.cc.cc.lib
          zlib
          openssl
        ] ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
          pkgs.libiconv
          pkgs.darwin.apple_sdk.frameworks.Security
          pkgs.darwin.apple_sdk.frameworks.CoreFoundation
        ];

        # 2. Common Tools
        devTools = with pkgs; [
          pkg-config
          python312
          python312Packages.pip
          python312Packages.virtualenv
          cargo
          rustc
          rust-analyzer
          maturin
          gcc
          zsh  # <--- Added ZSH here
        ];

        # 3. Common Shell Hook
        baseShellHook = ''
          export LD_LIBRARY_PATH=${pkgs.lib.makeLibraryPath buildLibraries}:$LD_LIBRARY_PATH
          unset SOURCE_DATE_EPOCH
        '';

      in
      {
        devShells = {
          # ---------------------------------------------------------
          # Shell 1: Default (Auto-setup)
          # usage: 'nix develop'
          # ---------------------------------------------------------
          default = pkgs.mkShell {
            nativeBuildInputs = devTools;
            buildInputs = buildLibraries;

            shellHook = baseShellHook + ''
              # 1. Auto-setup logic
              if [ ! -d ".venv" ]; then
                echo "Creating virtual environment..."
                python -m venv .venv
              fi

              # 2. Activate venv (Sets PATH and VIRTUAL_ENV)
              source .venv/bin/activate

              # 3. Install requirements
              if [ -f "requirements.txt" ]; then
                echo "Syncing requirements..."
                pip install -r requirements.txt
              fi
            '';
          };

          # ---------------------------------------------------------
          # Shell 2: Python (Manual/Lightweight)
          # usage: 'nix develop .#python'
          # ---------------------------------------------------------
          python = pkgs.mkShell {
            nativeBuildInputs = devTools;
            buildInputs = buildLibraries;

            shellHook = baseShellHook + ''
              source .venv/bin/activate
            '';
          };
        };
      }
    );
}