# Copyright (C) 2021-present ScyllaDB
#

#
# This file is part of Scylla.
#
# See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.

{
  pkgs ? null,
  mode ? "dev",
  useCcache ? false
}:
import ./default.nix ({
  inherit mode useCcache;
  testInputsFrom = pkgs: with pkgs; [
    python3Packages.boto3
    python3Packages.colorama
    python3Packages.pytest
  ];
  gitPkg = pkgs: pkgs.gitFull;
} //
(if pkgs != null then { inherit pkgs; } else {}))
