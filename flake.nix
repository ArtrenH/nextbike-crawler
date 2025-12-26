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

        __pythonPkg = (pkgs.python312.withPackages (ps: with ps; [
#          pip
          virtualenv
#          setuptools
#          wheel
#          cython
          tkinter
        ]));

        # 2. Common Tools
        devTools = with pkgs; [
          pkg-config
          __pythonPkg
          cargo
          rustc
          rust-analyzer
          maturin
          gcc
          tk
          tcl
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
          # usage: 'nix develop -c $SHELL'
          # ---------------------------------------------------------
          default = pkgs.mkShell {
            nativeBuildInputs = devTools;
            buildInputs = buildLibraries;

            shellHook = baseShellHook + ''
              export VENV_DIR="$PWD/.venv"

              # 1. Auto-setup logic
              if [ ! -d "$VENV_DIR" ]; then
                echo "Creating virtual environment..."
                ${__pythonPkg}/bin/python -m venv $VENV_DIR
              fi

              # 2. Activate venv (Sets PATH and VIRTUAL_ENV)
              source $VENV_DIR/bin/activate

              # Set up paths
              export PYTHONPATH="${__pythonPkg}/lib/${__pythonPkg.libPrefix}/site-packages:$PYTHONPATH"
              export TCL_LIBRARY="${pkgs.tcl}/lib/tcl${pkgs.lib.versions.majorMinor pkgs.tcl.version}"
              export TK_LIBRARY="${pkgs.tk}/lib/tk${pkgs.lib.versions.majorMinor pkgs.tk.version}"

              pip install --upgrade pip

              # 3. Install requirements
              if [ -f "requirements.txt" ]; then
                echo "Installing requirements..."
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
              export VENV_DIR="$PWD/.venv"

              source $VENV_DIR/bin/activate

              # Set up paths
              export PYTHONPATH="${__pythonPkg}/lib/${__pythonPkg.libPrefix}/site-packages:$PYTHONPATH"
              export TCL_LIBRARY="${pkgs.tcl}/lib/tcl${pkgs.lib.versions.majorMinor pkgs.tcl.version}"
              export TK_LIBRARY="${pkgs.tk}/lib/tk${pkgs.lib.versions.majorMinor pkgs.tk.version}"
            '';
          };
        };
      }
    );
}
