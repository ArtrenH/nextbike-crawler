#!/bin/sh
SCRIPT_PATH=$(dirname "$0")
FLAKE_PATH=$(realpath "$SCRIPT_PATH/..")
exec nix develop "$FLAKE_PATH#python" --command python "$@"
