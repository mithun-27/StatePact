"""Wait-for graph construction and cycle detection.

The graph is derived on demand from lock ownership. No background thread is
needed: a cycle can only be created when a transaction starts waiting, so checking
at that moment is sufficient.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Set
from dataclasses import dataclass, field

Node = str


@dataclass
class WaitForGraph:
    edges: dict[Node, set[Node]] = field(default_factory=dict)

    def add_edge(self, waiter: Node, blocker: Node) -> None:
        if waiter == blocker:
            return
        self.edges.setdefault(waiter, set()).add(blocker)

    def add_edges(self, waiter: Node, blockers: Set[Node]) -> None:
        for blocker in blockers:
            self.add_edge(waiter, blocker)

    def neighbours(self, node: Node) -> Set[Node]:
        return self.edges.get(node, set())

    def find_cycle(self, start: Node) -> list[Node] | None:
        """Return a cycle starting and ending at ``start``, or ``None``.

        Iterative DFS with an explicit path keeps recursion limits out of the
        failure path; agent fleets can be large.
        """

        if start not in self.edges:
            return None
        stack: list[tuple[Node, Iterator[Node]]] = [
            (start, iter(sorted(self.edges.get(start, ()))))
        ]
        path: list[Node] = [start]
        on_path: set[Node] = {start}
        while stack:
            node, iterator = stack[-1]
            advanced = False
            for neighbour in iterator:
                if neighbour == start:
                    return [*path, start]
                if neighbour in on_path:
                    continue
                if neighbour in self.edges:
                    path.append(neighbour)
                    on_path.add(neighbour)
                    stack.append((neighbour, iter(sorted(self.edges[neighbour]))))
                    advanced = True
                    break
            if not advanced:
                stack.pop()
                on_path.discard(node)
                if path and path[-1] == node:
                    path.pop()
        return None

    def any_cycle(self) -> list[Node] | None:
        for node in sorted(self.edges):
            cycle = self.find_cycle(node)
            if cycle is not None:
                return cycle
        return None

    @classmethod
    def from_blockers(cls, blockers: Mapping[Node, Set[Node]]) -> WaitForGraph:
        graph = cls()
        for waiter, holders in blockers.items():
            graph.add_edges(waiter, holders)
        return graph

    def to_dict(self) -> dict[str, list[str]]:
        return {node: sorted(targets) for node, targets in sorted(self.edges.items())}

    def __len__(self) -> int:
        return len(self.edges)
