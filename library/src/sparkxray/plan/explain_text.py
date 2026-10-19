"""Read a physical plan from explain text, in simple, extended or formatted mode.

Spark draws a tree with `+- ` and `:- ` markers, and each node's text starts three columns right
of its parent's. Two things break that rule, so a parent is the nearest open node to the left
rather than the node exactly three columns left:

- Inner children, such as a cached plan under `InMemoryTableScan` or a subquery in simple mode,
  are drawn six columns right.
- `AdaptiveSparkPlan` draws its `== Final Plan ==` and `== Initial Plan ==` headers at a column
  computed from its depth alone. Each block's root starts at its header's text column, which can
  equal the column of the `AdaptiveSparkPlan` that owns it.
"""

from __future__ import annotations

import dataclasses as dc
import re

from sparkxray.plan import labels
from sparkxray.plan import tree

_PHYSICAL_HEADER = '== Physical Plan =='
_LOGICAL_HEADER = re.compile(r'== (?:Parsed|Analyzed|Optimized) Logical Plan ==\s*')
_WHOLE_PLAN_TRUNCATED = re.compile(r'\s*Truncated plan of \d+ characters\s*$')
_CUT_SUFFIX = re.compile(r'\.\.\. \d+ more characters\s*$', re.MULTILINE)
_TREE_LINE = re.compile(r'(?P<lead>[ :]*?)(?P<marker>[+:]- )?(?P<body>[^ :].*)')
_BLOCK_HEADER = re.compile(r'== (Final|Current|Initial) Plan ==')
_FORMATTED_BODY = re.compile(r'(?P<codegen>\* )?(?P<name>.+?) \((?P<number>\d+)\)(?:, (?P<stats>Statistics\(.*\)))?\s*')
_SIMPLE_BODY = re.compile(r'(?:\*(?:\((?P<stage>\d+)\))? ?)?(?P<rest>.*)', re.DOTALL)
_DETAILS_HEADER = re.compile(
    r'\((?P<number>\d+)\) (?P<name>.*?)'
    r'(?: \[(?:codegen id : (?P<codegen>\d+)|Reuses operator id: (?P<reuses>\d+))\])?\s*'
)
_DETAILS_LINE = re.compile(r'(?P<key>[A-Za-z][A-Za-z ]*?)(?: \[\d+\])? ?: (?P<value>.*)')
_SUBQUERY_ENTRY = re.compile(r'Subquery:\d+ Hosting operator id = (?P<host>\d+) Hosting Expression = (?P<expression>.*)')
_SUBPLAN_ENTRY = re.compile(r'Subplan:\d+\s*')
_STATISTIC = re.compile(r'(\w+)=([^,)]+)')
_INNER_CHILD_OFFSET = 6
_KEPT_INNER_CHILDREN = labels.SUBQUERY_OPERATORS | {'InMemoryRelation'}
_SIZE_UNITS = {'B': 1, 'KiB': 2**10, 'MiB': 2**20, 'GiB': 2**30, 'TiB': 2**40, 'PiB': 2**50, 'EiB': 2**60}
_LONG_MAX_PLACEHOLDER = 8 * 2**60


def parse_plan_text(text: str, *, execution_id: int | None = None, version: int = 0) -> tree.Plan:
    """Read explain text, or an event log's `physicalPlanDescription`, telling simple from formatted.

    Extended text is read from its `== Physical Plan ==` section. In AQE text the final or current
    block under the first `AdaptiveSparkPlan` is the plan, and its initial block is `initial()`. A
    nested `AdaptiveSparkPlan`, as in a cached relation, keeps its final block only, which is what
    the JSON tree holds. `InMemoryRelation` is folded away, because the JSON tree never shows it.
    """
    raw, source = raw_plan_text(text)
    return tree.freeze(raw, source=source, execution_id=execution_id, version=version)


@dc.dataclass(eq=False)
class _Tree:
    """One parsed tree, with each `AdaptiveSparkPlan`'s blocks held aside until they are placed."""

    root: tree.RawNode | None
    nodes: list[tree.RawNode]
    blocks: dict[int, dict[str, tree.RawNode]]
    inner: set[int]


def raw_plan_text(text: str) -> tuple[tree.RawPlan, tree.PlanSource]:
    truncated = labels.is_truncated(text)
    if _WHOLE_PLAN_TRUNCATED.match(text):
        return tree.RawPlan(root=None, is_final=False, truncated=True), 'formatted_text'
    lines = _physical_section(_CUT_SUFFIX.sub('', text))
    while lines and not lines[0].strip():
        lines.pop(0)
    end = next((index for index, line in enumerate(lines) if not line.strip()), len(lines))
    tree_lines, rest = lines[:end], lines[end:]
    first = _TREE_LINE.fullmatch(tree_lines[0]) if tree_lines else None
    formatted = first is not None and _FORMATTED_BODY.fullmatch(first['body']) is not None
    source: tree.PlanSource = 'formatted_text' if formatted else 'simple_text'

    main = _build_tree(tree_lines, formatted)
    trees = [main]
    if formatted:
        details, entries = _read_sections(rest)
        subqueries = []
        for host, expression, entry_lines in entries:
            subquery_tree = _build_tree(entry_lines, formatted=True) if entry_lines else None
            subqueries.append((host, expression, subquery_tree))
            if subquery_tree is not None:
                trees.append(subquery_tree)
        by_number: dict[int, list[tree.RawNode]] = {}
        for parsed in trees:
            for node in parsed.nodes:
                if node.formatted_number is not None:
                    by_number.setdefault(node.formatted_number, []).append(node)
        for number, (codegen, block) in details.items():
            for node in by_number.get(number, []):
                scan_format = node.details.get('Format')
                node.details.update(block)
                if scan_format:
                    node.details['Format'] = scan_format
                node.codegen_stage = codegen
        for host, expression, subquery_tree in subqueries:
            ids = labels.subquery_ids([expression])
            root = None
            is_final = True
            if subquery_tree is not None and not expression.startswith('ReusedSubquery'):
                root = _place_blocks(subquery_tree, keep_initial=False)[0]
                is_final = _is_final(root, subquery_tree)
            for node in by_number.get(host, []):
                node.hosted.append(tree.RawSubquery(subquery_id=ids[-1] if ids else None, root=root, is_final=is_final))

    root, initial_root = _place_blocks(main, keep_initial=True)
    inner = set().union(*(parsed.inner for parsed in trees))
    for parsed_root in (root, initial_root):
        _drop_logical_inner_children(parsed_root, inner)
        _fold_in_memory_relations(parsed_root)
        if not formatted:
            _extract_inline_subqueries(parsed_root)
    initial = None
    if initial_root is not None:
        initial = tree.RawPlan(root=initial_root, is_final=False, truncated=truncated)
    is_final = _is_final(root, main) if root is not None else False
    return tree.RawPlan(root=root, is_final=is_final, truncated=truncated, initial=initial), source


def _physical_section(text: str) -> list[str]:
    lines = text.split('\n')
    for index, line in enumerate(lines):
        if line.strip() == _PHYSICAL_HEADER:
            section = lines[index + 1 :]
            for end, candidate in enumerate(section):
                if candidate.startswith('== ') and candidate.rstrip().endswith('=='):
                    return section[:end]
            return section
    if any(_LOGICAL_HEADER.fullmatch(line) for line in lines):
        return []
    return lines


def _build_tree(lines: list[str], formatted: bool) -> _Tree:
    parsed = _Tree(root=None, nodes=[], blocks={}, inner=set())
    stack: list[tuple[int, tree.RawNode]] = []
    pending: tuple[tree.RawNode, str] | None = None
    last: tree.RawNode | None = None
    for line in lines:
        match = _TREE_LINE.fullmatch(line.rstrip())
        if match is None:
            continue
        column = len(match['lead']) + len(match['marker'] or '')
        body = match['body']
        header = _BLOCK_HEADER.fullmatch(body)
        if header:
            owner = next((node for start, node in reversed(stack) if start <= column and node.operator == 'AdaptiveSparkPlan'), None)
            if owner is not None:
                while stack[-1][1] is not owner:
                    stack.pop()
                pending = (owner, header.group(1))
            continue
        if match['marker'] is None and pending is None and parsed.root is not None:
            if last is not None:
                last.label += '\n' + body
            continue
        node = _node(body, formatted)
        parsed.nodes.append(node)
        last = node
        if pending is not None:
            owner, kind = pending
            pending = None
            parsed.blocks.setdefault(id(owner), {})[kind] = node
        elif parsed.root is None:
            parsed.root = node
        else:
            while len(stack) > 1 and stack[-1][0] >= column:
                stack.pop()
            parent_column, parent = stack[-1]
            parent.children.append(node)
            if column - parent_column >= _INNER_CHILD_OFFSET:
                parsed.inner.add(id(node))
        stack.append((column, node))
    return parsed


def _node(body: str, formatted: bool) -> tree.RawNode:
    if formatted:
        match = _FORMATTED_BODY.fullmatch(body)
        if match is not None:
            label = body[2:] if match['codegen'] else body
            operator, scan_format = labels.canonical_operator(match['name'])
            node = tree.RawNode(label=label, operator=operator, formatted_number=int(match['number']))
            if match['stats']:
                node.runtime_stats = _statistics(match['stats'])
            if scan_format:
                node.details['Format'] = scan_format
            return node
    match = _SIMPLE_BODY.fullmatch(body)
    assert match is not None
    label = match['rest']
    operator, scan_format = labels.canonical_operator(label)
    node = tree.RawNode(label=label, operator=operator, codegen_stage=int(match['stage']) if match['stage'] else None)
    if scan_format:
        node.details['Format'] = scan_format
    if operator.endswith('Join'):
        found = labels.join_type(label)
        if found:
            node.details['Join type'] = found
    return node


def _place_blocks(parsed: _Tree, *, keep_initial: bool) -> tuple[tree.RawNode | None, tree.RawNode | None]:
    """Hang each AdaptiveSparkPlan's final or current block under it, and build the initial plan.

    The first `AdaptiveSparkPlan` in pre-order owns the plan's initial block. The initial plan is
    the same tree above that node, with the initial block below it.
    """
    if parsed.root is None:
        return None, None
    owners: list[tree.RawNode] = []

    def place(node: tree.RawNode) -> None:
        if node.operator == 'AdaptiveSparkPlan':
            owners.append(node)
            blocks = parsed.blocks.get(id(node), {})
            current = blocks.get('Final') or blocks.get('Current')
            if current is not None:
                node.children = [current]
        for child in node.children:
            place(child)

    place(parsed.root)
    top_initial = parsed.blocks.get(id(owners[0]), {}).get('Initial') if owners else None
    if not keep_initial or top_initial is None:
        return parsed.root, None
    place(top_initial)
    return parsed.root, _replace_subtree(parsed.root, owners[0], top_initial)


def _replace_subtree(node: tree.RawNode, owner: tree.RawNode, initial_root: tree.RawNode) -> tree.RawNode:
    if node is owner:
        return dc.replace(node, children=[initial_root])
    return dc.replace(
        node, children=[_replace_subtree(child, owner, initial_root) if _contains(child, owner) else child for child in node.children]
    )


def _contains(node: tree.RawNode, target: tree.RawNode) -> bool:
    return any(candidate is target for candidate in tree.raw_nodes(node, subqueries=False))


def _is_final(root: tree.RawNode | None, parsed: _Tree) -> bool:
    for node in tree.raw_nodes(root, subqueries=False):
        if node.operator == 'AdaptiveSparkPlan':
            blocks = parsed.blocks.get(id(node), {})
            return 'Final' in blocks or 'isFinalPlan=true' in node.label or node.details.get('Arguments') == 'isFinalPlan=true'
    return True


def _drop_logical_inner_children(root: tree.RawNode | None, inner: set[int]) -> None:
    """Drop the logical plans Spark draws as inner children, such as a command's, which JSON omits.

    A cached relation and a simple-mode subquery are inner children too, and both are kept.
    """
    for node in tree.raw_nodes(root):
        if node.operator != 'InMemoryRelation':
            node.children = [child for child in node.children if id(child) not in inner or child.operator in _KEPT_INNER_CHILDREN]


def _fold_in_memory_relations(root: tree.RawNode | None) -> None:
    for node in tree.raw_nodes(root):
        if any(child.operator == 'InMemoryRelation' for child in node.children):
            folded: list[tree.RawNode] = []
            for child in node.children:
                folded.extend(child.children if child.operator == 'InMemoryRelation' else [child])
            node.children = folded


def _extract_inline_subqueries(root: tree.RawNode | None) -> None:
    """Simple text draws a subquery as an inner child of the node hosting it, as the JSON tree does."""
    for node in list(tree.raw_nodes(root, subqueries=False)):
        kept = []
        for child in node.children:
            if child.operator not in labels.SUBQUERY_OPERATORS:
                kept.append(child)
                continue
            ids = labels.subquery_ids([child.label])
            subquery_root = None if child.operator == 'ReusedSubquery' or not child.children else child.children[0]
            is_final = True
            if subquery_root is not None:
                _extract_inline_subqueries(subquery_root)
                is_final = next(
                    (
                        'isFinalPlan=true' in candidate.label
                        for candidate in tree.raw_nodes(subquery_root)
                        if candidate.operator == 'AdaptiveSparkPlan'
                    ),
                    True,
                )
            node.hosted.append(tree.RawSubquery(subquery_id=ids[0] if ids else None, root=subquery_root, is_final=is_final))
        node.children = kept


def _read_sections(lines: list[str]) -> tuple[dict[int, tuple[int | None, dict[str, str]]], list[tuple[int, str, list[str]]]]:
    """Read the details blocks and subquery entries that follow a formatted tree."""
    details: dict[int, tuple[int | None, dict[str, str]]] = {}
    entries: list[tuple[int, str, list[str]]] = []
    index = 0
    previous_blank = True
    while index < len(lines):
        line = lines[index].rstrip()
        subquery = _SUBQUERY_ENTRY.fullmatch(line)
        if subquery or _SUBPLAN_ENTRY.fullmatch(line):
            tree_lines = []
            index += 1
            while index < len(lines) and lines[index].strip():
                tree_lines.append(lines[index])
                index += 1
            if subquery:
                entries.append((int(subquery['host']), subquery['expression'], tree_lines))
            previous_blank = False
            continue
        header = _DETAILS_HEADER.fullmatch(line) if previous_blank else None
        if header:
            block: dict[str, str] = {}
            if header['reuses']:
                block['Reuses operator id'] = header['reuses']
            key = None
            index += 1
            while index < len(lines) and lines[index].strip():
                item = _DETAILS_LINE.fullmatch(lines[index].rstrip())
                if item:
                    key = item['key']
                    block[key] = item['value']
                elif key is not None:
                    block[key] += '\n' + lines[index].rstrip()
                index += 1
            details[int(header['number'])] = (int(header['codegen']) if header['codegen'] else None, block)
            previous_blank = False
            continue
        previous_blank = not line.strip()
        index += 1
    return details, entries


def _statistics(text: str) -> tree.RuntimeStats:
    values = dict(_STATISTIC.findall(text))
    size_bytes = None
    size = values.get('sizeInBytes', '').split()
    if len(size) == 2 and size[1] in _SIZE_UNITS:
        size_bytes = round(float(size[0]) * _SIZE_UNITS[size[1]])
        if size_bytes >= _LONG_MAX_PLACEHOLDER:
            size_bytes = None
    row_count = None
    if 'rowCount' in values:
        row_count = round(float(values['rowCount']))
    return tree.RuntimeStats(size_bytes=size_bytes, row_count=row_count)
