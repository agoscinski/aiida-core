"""Run the gallery examples in disposable profiles and render their stored links."""

from __future__ import annotations

import base64
import importlib
import json
import os
import subprocess
from html import escape
from pathlib import Path

import pytest
from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import PythonLexer

from aiida import orm
from aiida.common.links import LinkType

HERE = Path(__file__).parent
TARGET = Path(os.environ.get('GALLERY_HTML', str(HERE.parent / 'provenance-gallery.html')))


def revision() -> str:
    """Identify the code snapshot used to run the examples."""
    if value := os.environ.get('GALLERY_REVISION'):
        return value
    result = subprocess.run(
        ['git', '-C', str(HERE), 'rev-parse', '--short', 'HEAD'],
        text=True,
        capture_output=True,
        check=False,
    )
    return result.stdout.strip() if result.returncode == 0 else 'unknown'

CASES = [
    ('writable_task', 'A writable task input', 'FINISHED', 'The input link points to the original structure. The task edits an unstored clone and creates a new structure; its cell volume changes from 1 to 125 Å³.'),
    ('nested_graphs', 'Forward write permission through a nested graph', 'FINISHED', 'Both report and prepare opt in. Only expand clones the structure. The nested prepare scope has no node of its own: prepare__expand is the child call label. The root returns both structures and creates and returns the inline count.'),
    ('independent_aliases', 'The same source feeds two writable ports', 'FINISHED', 'The first and second input links point to the same UUID. The two ports receive independent clones, so they can publish structures with different volumes (8 and 27 Å³).'),
    ('missing_opt_in', 'A nested scope drops write permission', 'BUILD REJECTED', 'The outer graph opts in, but prepare does not. Build rejects the read-only → writable binding before launching anything.'),
    ('readonly_mutation', 'A setter on a read-only task input', 'EXCEPTED', 'The task runs with the original stored node. The setter raises ModificationNotAllowed. The task record and its input link remain, but there are no output links.'),
    ('discarded_copy', 'The task discards its working copy', 'FINISHED', 'The expanded structure never enters provenance because the task does not publish it. Only its derived scalar, a volume of 64 Å³, is created.'),
    ('equal_value', 'Equal values are not the same node', 'FINISHED', 'Both integers have value 7. The graph returns the existing UUID unchanged, but materializes value + 0 as a new UUID and records both CREATE and RETURN for it.'),
]

LINK_STYLES = {
    LinkType.INPUT_CALC: ('#245aa7', 'solid'),
    LinkType.INPUT_WORK: ('#245aa7', 'solid'),
    LinkType.CALL_CALC: ('#6543b2', 'dotted'),
    LinkType.CALL_WORK: ('#6543b2', 'dotted'),
    LinkType.CREATE: ('#14734b', 'bold'),
    LinkType.RETURN: ('#ad4b14', 'dashed'),
}


def render(root: orm.ProcessNode, diagram_id: str) -> tuple[str, int, int]:
    """Render stored links, not inferred dependencies or relationships from the DSL."""
    nodes = {}
    edges = set()
    processes = [root]
    while processes:
        process = processes.pop(0)
        nodes[process.pk] = process
        for link in process.base.links.get_incoming(link_type=(LinkType.INPUT_CALC, LinkType.INPUT_WORK)).all():
            if isinstance(process, orm.GraphNode) and link.link_label in {'graph', 'runtime_config'}:
                continue
            nodes[link.node.pk] = link.node
            edges.add((link.node.pk, process.pk, link.link_type, link.link_label))
        for link in process.base.links.get_outgoing(link_type=tuple(LINK_STYLES)).all():
            nodes[link.node.pk] = link.node
            edges.add((process.pk, link.node.pk, link.link_type, link.link_label))
            if link.link_type in (LinkType.CALL_CALC, LinkType.CALL_WORK):
                processes.append(link.node)

    rankdir = 'TB' if len(edges) > 4 else 'LR'
    dot = [
        'digraph provenance {',
        f'graph [rankdir={rankdir}, fontname="Helvetica", bgcolor="transparent", pad="0.12", nodesep="0.35", ranksep="0.56", splines=polyline];',
        'node [shape=box, style="rounded,filled", fontsize=13, fontname="Helvetica", margin="0.14,0.12", color="#c6d4db", fillcolor="#f1f6f8", fontcolor="#152631"];',
        'edge [fontsize=12, fontname="Helvetica", arrowsize=0.65, penwidth=1.25];',
    ]
    for pk, node in sorted(nodes.items()):
        info = [type(node).__name__]
        args = ''
        if isinstance(node, orm.GraphNode):
            info.append(node.process_label)
            args = ', fillcolor="#eaf0ff", color="#7093df", penwidth=1.5'
        elif isinstance(node, orm.ProcessNode):
            info.append(node.process_label)
            args = ', fillcolor="#f2ebff", color="#a082d4", penwidth=1.5'
        elif isinstance(node, orm.StructureData):
            info.append(f'V = {node.get_cell_volume():.6g} Å³')
        elif isinstance(node, (orm.Int, orm.Float)):
            info.append(f'value = {node.value:.6g}')
        info.append(f'PK {pk} · {node.uuid[:8]}')
        dot.append(f'n{pk} [label={json.dumps(chr(10).join(info), ensure_ascii=False)}{args}];')

    for source, target, link_type, link_label in sorted(edges, key=lambda item: (item[0], item[1], item[2].value, item[3])):
        color, style = LINK_STYLES[link_type]
        label = json.dumps(f'{link_type.name}\n{link_label}')
        dot.append(f'n{source} -> n{target} [label={label}, color="{color}", fontcolor="{color}", style="{style}"];')
    dot.append('}')
    svg = subprocess.run(['dot', '-Tsvg'], input='\n'.join(dot), text=True, capture_output=True, check=True).stdout
    svg = svg[svg.index('<svg'):]
    svg = svg.replace('<svg ', '<svg role="img" aria-label="Stored AiiDA provenance links" ', 1)
    svg = svg.replace(' id="', f' id="svg-{diagram_id}-')
    return svg, len(nodes), len(edges)


STYLE = '''
:root{color-scheme:light;--ink:#142635;--muted:#4c6573;--paper:#f8fafb;--line:#d9e4e9;--accent:#0c7058}
*{box-sizing:border-box}html{scroll-behavior:smooth;scroll-padding-top:4rem}
body{margin:0;background:#edf2f4;color:var(--ink);font:15px/1.55 system-ui,-apple-system,BlinkMacSystemFont,sans-serif}
a{color:#075d49}a:hover{text-decoration-thickness:2px}a:focus-visible{outline:2px solid #08735a;outline-offset:3px}
main{width:min(1640px,calc(100% - 48px));margin:26px auto 54px}
header{color:#f5fffd;background:#12322e;border-radius:20px;padding:clamp(22px,4vw,50px);border:1px solid #16423b}
.eyebrow,.panel-heading,.overline{font-size:.73rem;letter-spacing:.14em;text-transform:uppercase;font-weight:760}
.eyebrow{color:#9cebd0;margin:0 0 .4rem}h1{font-size:clamp(2.1rem,5vw,4.3rem);line-height:1.02;letter-spacing:-.05em;margin:0 0 .75rem}
header p{max-width:68ch;margin:.5rem 0;color:#d9eee8}header code{color:#b1f6db}
nav{display:flex;gap:7px;flex-wrap:wrap;margin:14px 0 18px}nav a{text-decoration:none;background:#fff;border:1px solid var(--line);border-radius:999px;padding:4px 11px;font-size:.83rem}nav a:hover{background:#e1faf0}
.legend{display:flex;gap:7px 16px;align-items:center;flex-wrap:wrap;padding:10px 15px;margin-bottom:15px;border:1px solid var(--line);border-radius:12px;background:#fff;color:var(--muted);font-size:.83rem}
.legend strong{color:var(--ink)}.legend span{white-space:nowrap}.sample{display:inline-block;width:21px;vertical-align:middle;margin-right:5px;border-top:2px solid currentColor}
.sample.input{color:#245aa7}.sample.call{color:#6543b2;border-top-style:dotted}.sample.create{color:#14734b;border-top-width:3px}.sample.return{color:#ad4b14;border-top-style:dashed}
table{width:100%;table-layout:fixed;border-spacing:0;border:1px solid var(--line);border-radius:12px;background:white}
thead th{position:sticky;top:0;z-index:1;text-align:left;background:#e1eaed;color:#284451;padding:10px 17px}
tbody th{padding:22px 20px 11px;background:#fff;text-align:left;border-top:1px solid var(--line);font-weight:400}
.heading{display:flex;gap:10px;align-items:center;flex-wrap:wrap}.overline{color:var(--accent)}h2{font-size:1.25rem;line-height:1.25;margin:0;letter-spacing:-.025em}
.badge{color:#176043;background:#daf2e6;padding:3px 7px;border-radius:4px;font-size:.67rem;letter-spacing:.07em;font-weight:750}
.badge.problem{color:#812e11;background:#ffeadc}.description{margin:8px 0 0;color:var(--muted);max-width:105ch}
td{vertical-align:top;padding:0 10px 18px}td:first-child{padding-left:20px}td:last-child{padding-right:20px}
.panel{background:var(--paper);border:1px solid var(--line);border-radius:9px;overflow:hidden}.panel-heading{display:flex;align-items:center;justify-content:space-between;gap:8px;min-height:44px;padding:7px 12px;color:var(--muted);border-bottom:1px solid var(--line)}.panel-heading a{font-size:.72rem;text-transform:none;letter-spacing:normal}
pre{margin:0;overflow:auto;padding:13px 12px 16px;font:12.5px/1.55 ui-monospace,SFMono-Regular,Consolas,monospace;tab-size:4}
.pre-graph{overflow:auto;padding:12px 10px 18px;min-height:152px;display:flex;align-items:center}.pre-graph svg{display:block;margin:auto;min-width:min(100%,440px);max-width:100%;height:auto}
.empty{width:100%;border:1px dashed #d6b598;border-radius:7px;background:#fff9f4;padding:12px 14px;color:#713d22;overflow-wrap:anywhere}.empty p{margin:.45rem 0 0}.empty code{font-size:.83rem;white-space:pre-wrap}
footer{padding:10px 3px;color:var(--muted);font-size:.82rem}.panel-heading .count{white-space:nowrap;letter-spacing:.04em}
@media(max-width:900px){main{width:calc(100% - 24px);margin:12px auto 28px}thead{display:none}table,tbody,tr,td,tbody th{display:block;width:100%}td,td:first-child,td:last-child{padding:0 12px 12px}tbody th{padding:16px 12px 10px}.pre-graph{min-height:112px}html{scroll-padding-top:.5rem}}
@media(prefers-reduced-motion:reduce){html{scroll-behavior:auto}}
'''


def test_render_gallery(aiida_config_tmp, aiida_profile_factory):
    fmt = HtmlFormatter(nowrap=True, style='friendly')
    rows = []
    nav = []
    for index, (module_name, title, status, description) in enumerate(CASES, 1):
        source_file = HERE / f'{module_name}.py'
        source = source_file.read_text(encoding='utf-8')
        module = importlib.import_module(module_name)
        with aiida_profile_factory(aiida_config_tmp):
            result = module.run_case()
            if isinstance(result, str):
                assert status == 'BUILD REJECTED'
                diagram = f'<div class="empty"><strong>No provenance was stored</strong><p><code>{escape(result)}</code></p></div>'
                nnodes = nedges = 0
            else:
                assert result.is_finished_ok if status == 'FINISHED' else result.is_excepted
                diagram, nnodes, nedges = render(result, module_name)
        nav.append(f'<a href="#{module_name}">{index:02d} {escape(title)}</a>')
        encoded = base64.b64encode(source.encode()).decode('ascii')
        badge = '' if status == 'FINISHED' else ' problem'
        rows.append(f'''<tbody id="{module_name}">
<tr><th colspan="2"><div class="heading"><span class="overline">Case {index:02d}</span><h2>{escape(title)}</h2><span class="badge{badge}">{status}</span></div><p class="description">{escape(description)}</p></th></tr>
<tr><td><div class="panel"><div class="panel-heading"><span>Source code · {module_name}.py</span><a href="data:text/x-python;charset=utf-8;base64,{encoded}" download="{module_name}.py">Download script</a></div><pre class="python"><code>{highlight(source, PythonLexer(), fmt)}</code></pre></div></td>
<td><div class="panel"><div class="panel-heading"><span>Stored provenance</span><span class="count">{nnodes} nodes · {nedges} links</span></div><div class="pre-graph">{diagram}</div></div></td></tr>
</tbody>''')

    TARGET.write_text(f'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>AiiDA provenance gallery · Graphs and writable tasks</title><style>{STYLE}\n{fmt.get_style_defs('.python')}</style></head><body><main>
<header><p class="eyebrow">AiiDA · Provenance gallery</p><h1>Graphs and writable tasks</h1><p>Seven Python examples, run in isolated SQLite profiles. The diagrams show the links AiiDA stored, including where an inline graph output carries both <code>CREATE</code> and <code>RETURN</code>.</p><p>Branch snapshot: <code>{escape(revision())}</code></p></header>
<nav aria-label="Jump to an example">{''.join(nav)}</nav>
<div class="legend" aria-label="Link styles"><strong>Links</strong><span><i class="sample input"></i>Input</span><span><i class="sample call"></i>Call</span><span><i class="sample create"></i>Create</span><span><i class="sample return"></i>Return</span><span>Node labels show the PK and first eight UUID characters.</span></div>
<table aria-label="Python source with matching AiiDA provenance diagrams"><colgroup><col style="width:43%"><col style="width:57%"></colgroup><thead><tr><th class="panel-heading-label" scope="col">Python source</th><th class="panel-heading-label" scope="col">Provenance graph</th></tr></thead>
{''.join(rows)}
</table><footer>Technical inputs (the serialized graph definition and runtime configuration) are omitted so the diagrams focus on scientific data. PKs belong to the temporary profiles. The downloadable examples define a <code>run_case()</code> function; call it from a configured AiiDA profile to reproduce the example. The build rejection case expects an empty profile.</footer></main></body></html>''', encoding='utf-8')
    assert TARGET.stat().st_size > 30_000


if __name__ == '__main__':
    result = pytest.main(['-q', '-p', 'aiida.tools.pytest_fixtures', str(Path(__file__).resolve())])
    if result == pytest.ExitCode.OK:
        print(f'Wrote {TARGET.resolve()}')
    raise SystemExit(result)
