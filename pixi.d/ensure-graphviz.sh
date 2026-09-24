#!/bin/bash
# Replicates conda-forge graphviz's post-link step (`dot -c`), which pixi
# (rattler) does not execute. Without the plugin registry
# (`$CONDA_PREFIX/lib/graphviz/config*`), `dot` fails with
# 'There is no layout engine support for "dot"'.
if ! compgen -G "${CONDA_PREFIX}/lib/graphviz"/config* > /dev/null; then
    "${CONDA_PREFIX}/bin"/dot -c || true
fi
