# AiiDA provenance gallery source

`../provenance-gallery.html` is a standalone report. It embeds the Python source, downloadable example scripts, styles, and SVG diagrams; no server or network connection is needed to view it.

From the repository root, regenerate it with:

```bash
uv run python provenance-gallery-src/generate_gallery.py
```

The generator requires this checkout of AiiDA, the project's pytest environment, Pygments, and the Graphviz `dot` executable on `PATH`.
It runs each example's `run_case()` in its own temporary SQLite profile and renders the stored ORM links to SVG before assembling the report.
New runs will have different node UUIDs.
To write to another location, set `GALLERY_HTML=/path/to/gallery.html`.

| Script | Example |
| --- | --- |
| `writable_task.py` | Copy-on-write task input |
| `nested_graphs.py` | A nested graph forwards write permission |
| `independent_aliases.py` | Two writable ports receive independent clones of the same source |
| `missing_opt_in.py` | Build-time rejection at a nested graph boundary |
| `readonly_mutation.py` | Runtime rejection of a setter on a stored input |
| `discarded_copy.py` | The working copy is not published |
| `equal_value.py` | Equal-valued outputs with different UUIDs and link types |
