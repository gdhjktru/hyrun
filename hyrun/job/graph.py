import json
from pathlib import Path
from typing import Callable, List, Optional

import networkx as nx
from hytools.graph import Graph
from hytools.logger import LoggerDummy

edge_keys = ['edge', 'edges', 'dependencies', 'dependency', 'dependencies']
node_keys = ['node', 'nodes', 'jobs', 'job', 'tasks', 'task']
weight_keys = ['weight', 'weights', 'weighting', 'weighting']
edge_attr = ['weight']
node_attr = ['hash', 'status', 'db_id']


class JobGraph(Graph):
    """Class to create a job graph."""

    def __init__(self, **kwargs):
        """Initialize job graph."""
        self.logger = kwargs.get('logger') or LoggerDummy()
        self.graph = nx.DiGraph()
        self._nodes = self.make_list(
            self.get_from_kwargs(kwargs, node_keys) or [])
        self._edges = self.make_list(
            self.get_from_kwargs(kwargs, edge_keys) or [])
        self.weights = self.make_list(kwargs.get('weights') or [])
        for n in self._nodes:
            self.add_node(n)
        for e in self._edges + self.weights:
            if isinstance(e, (tuple, list)):
                u, v = e[0], e[1]
                attr_dict = {}
                for i, attr in enumerate(edge_attr, start=2):
                    if len(e) > i:
                        attr_dict[attr] = e[i]
                self.add_edge(u, v, **attr_dict)
            elif isinstance(e, dict):
                u, v = list(e.keys())[0], list(e.values())[0]
                attr_dict = {k: v for k, v in e.items() if k in edge_attr}
                self.add_edge(u, v, **attr_dict)

    def get_from_kwargs(self, kwargs, keys):
        """Get value from kwargs."""
        for key in keys:
            if key in kwargs:
                return kwargs[key]
        return None

    def make_list(self, obj):
        """Make object a list."""
        if not isinstance(obj, list):
            obj = [obj]
        return obj

    def relabel_nodes(self, mapping):
        """Relabel nodes in graph."""
        self.graph = nx.relabel_nodes(self.graph, mapping)

    def add_node(self, node, keys: Optional[List[str]] = None):
        """Add node to graph."""
        idx = getattr(node, 'hash', None) or len(self.graph.nodes)
        self.logger.debug(f'Adding node {idx} to graph')
        if idx in self.graph.nodes:
            self.logger.error(f'Node {idx} already exists in graph')
            return
        self.graph.add_node(idx)
        keys = keys or node_attr
        for attr in keys:
            if hasattr(node, attr):
                self.graph.nodes[idx][attr] = getattr(node, attr, None)

    def add_edge(self, u, v, **kwargs):
        """Add edge to graph."""
        self.logger.debug(f'Adding edge {u} -> {v} to graph')
        if (u, v) in self.graph.edges:
            return
        self.graph.add_edge(u, v)
        for key, value in kwargs.items():
            if key not in self.graph.edges[u, v]:
                self.logger.error(
                    f'Key {key} does not exist in edge {u} -> {v}')
                return
            self.graph.edges[u, v][key] = value

    # def update(self, **kwargs):
    #     """Update graph."""
    #     new_nodes = self.make_list(
    #         self.get_from_kwargs(kwargs, node_keys) or [])
    #     for node in new_nodes:
    #         self.update_node(node)

    #     new_deps = self.make_list(
    #         self.get_from_kwargs(kwargs, edge_keys) or [])
    #     for (u, v) in new_deps:
    #         self.update_edge(u, v, **kwargs)

    # def update_node(self, node=None, keys=None):
    #     """Update node attributes in the graph from a list of job objects."""
    #     if not node:
    #         return
    #     keys = keys or node_attr
    #     idx = getattr(node, 'hash', None)
    #     self.logger.debug(f"Updating node {idx} in graph")
    #     if idx in self.graph.nodes:
    #         for attr in keys:
    #             if hasattr(node, attr):
    #                 self.graph.nodes[idx][attr] = getattr(node, attr)
    #                 self._nodes[idx][attr] = getattr(node, attr)
    #     else:
    #         self.logger.error(f"Node {idx} does not exist in graph")
    #         self.add_node(node)
    #         self._nodes = self.graph.nodes

    # def update_edge(self, u, v, **kwargs):
    #     """Update edge in graph."""
    #     idx = getattr(u, 'hash', None) or u
    #     idx2 = getattr(v, 'hash', None) or v
    #     if not idx or not idx2:
    #         return
    #     self.logger.debug(f"Updating edge {idx} -> {idx2} in graph")
    #     if (idx, idx2) in self.graph.edges:
    #         for key, value in kwargs.items():
    #             if key not in self.graph.edges[idx, idx2]:
    #                 self.logger.error(
    #                     f"Key {key} does not exist in edge {idx} -> {idx2}")
    #                 return
    #             self.graph.edges[idx, idx2][key] = value
    #             self._edges[idx, idx2][key] = value
    #     elif (idx2, idx) in self.graph.edges:
    #         for key, value in kwargs.items():
    #             if key not in self.graph.edges[idx2, idx]:
    #                 self.logger.error(
    #                     f"Key {key} does not exist in edge {idx2} -> {idx}")
    #                 return
    #             self.graph.edges[idx2, idx][key] = value
    #             self._edges[idx2, idx][key] = value
    #     else:
    #         self.logger.error(f"Edge {u} -> {v} does not exist in graph")
    #         self.add_edge(u, v, **kwargs)
    #         self._edges = self.graph.edges

    def remove_node(self, node):
        """Remove node from graph."""
        self.logger.debug(f'Removing node {node} from graph')
        if node not in self.graph.nodes:
            self.logger.error(f'Node {node} does not exist in graph')
            return
        self.graph.remove_node(node)

    def remove_edge(self, u, v, remove_descendants=False):
        """Remove edge from graph."""
        self.logger.debug(f'Removing edge {u} -> {v} from graph')
        if (u, v) not in self.graph.edges:
            self.logger.error(f'Edge {u} -> {v} does not exist in graph')
            return
        descendants = list(nx.descendants(self.graph, v))
        self.graph.remove_edge(u, v)
        if remove_descendants:
            self.logger.debug(f'Removing descendants of {v} from graph')
            self.graph.remove_node(v)
            for d in descendants:
                self.graph.remove_node(d)

    def subgraph(self,
                 filter_node: Optional[Callable] = None,
                 filter_edge: Optional[Callable] = None):
        """Create subgraph."""
        if filter_node is None and filter_edge is None:
            return self.graph
        subgraph = JobGraph()
        if filter_node:
            s = nx.subgraph_view(self.graph, filter_node=filter_node).nodes()
        if filter_edge:
            s = nx.subgraph_view(self.graph, filter_edge=filter_edge).edges()
        if filter_node and filter_edge:
            s = nx.subgraph_view(self.graph, filter_node=filter_node,
                                 filter_edge=filter_edge).nodes()
        subgraph.graph = self.graph.subgraph(s)
        return subgraph

    def __add__(self, other):
        """Add two graphs."""
        if not isinstance(other, JobGraph):
            raise ValueError('Can only add JobGraph objects.')
        new_graph = JobGraph()
        new_graph.graph = nx.compose(self.graph, other.graph)
        return new_graph

    def __sub__(self, other):
        """Subtract two graphs."""
        if not isinstance(other, JobGraph):
            raise ValueError('Can only subtract JobGraph objects.')
        new_graph = JobGraph()
        new_graph.graph = nx.difference(self.graph, other.graph)
        return new_graph

    # def subgraph_with_node_prop(self, **kwargs):
    #     """Return subgraph with nodes matching all filters."""
    #     def filter_node(n):
    #         return all(self.graph.nodes[n].get(k, None) == v
    #                    for k, v in kwargs.items())
    #     return self.subgraph(filter_node=filter_node)

    # def subgraph_with_edge_prop(self, **kwargs):
    #     """Return subgraph with edges matching all filters."""
    #     def filter_edge(u, v):
    #         return all(self.graph.edges[u, v].get(k, None) == v
    #                    for k, v in kwargs.items())
    #     return self.subgraph(filter_edge=filter_edge)

    def __iter__(self):
        """Iterate over graph."""
        return iter(self.graph.nodes)

    def __copy__(self):
        """Copy graph."""
        new_graph = JobGraph()
        new_graph.graph = self.graph.copy()
        return new_graph

    def __getitem__(self, item):
        """Get item from graph."""
        if isinstance(item, tuple):
            return self.graph.edges[list(item)]
        return self.graph.nodes[item]

    def __len__(self):
        """Get length of graph."""
        return len(self.graph.nodes)

    def __contains__(self, item):
        """Check if item is in graph."""
        return item in self.graph.nodes

    def write(self, filename=None) -> Optional[str]:
        """Dump graph to file."""
        graph_str = json.dumps(nx.node_link_data(self.graph, edges='edges'),
                               indent=4)
        if filename is None:
            return graph_str
        else:
            Path(filename).write_text(graph_str)
        return None

    def read(self, filename):
        """Read graph from file."""
        self.graph = nx.node_link_graph(json.load(open(filename, 'r')),
                                        edges='edges')

    def __str__(self) -> str:
        """Get string representation of graph."""
        return '\n'.join(nx.generate_network_text(self.graph))

    def ancestors(self, node):
        """Get all dependencies of a node."""
        return list(nx.ancestors(self.graph, node))

    def descendants(self, node):
        """Get all dependents of a node."""
        return list(nx.descendants(self.graph, node))

    @property
    def nodes(self):
        """Get all nodes in graph."""
        return list(self.graph.nodes)

    @property
    def edges(self):
        """Get all edges in graph."""
        return list(self.graph.edges)

    @property
    def topological(self):
        """Get topological order of graph."""
        return list(nx.topological_sort(self.graph))

    @property
    def map_topology(self):
        """Get map of topology."""
        node_to_index = {n: i for i, n in enumerate(self._nodes)}
        return {node_to_index[node]: i
                for i, node in enumerate(self.topological)}

    def show(self, title=None, node_color=None, node_size=None,
             node_shape=None):
        """Show graph."""
        title = title or 'Job Graph'
        node_color = node_color or 'white'
        node_size = node_size or 600
        node_shape = node_shape or 's'
        import matplotlib.pyplot as plt
        for layer, nodes in enumerate(nx.topological_generations(self.graph)):
            for node in nodes:
                self.graph.nodes[node]['layer'] = layer
        pos = nx.multipartite_layout(self.graph, subset_key='layer')
        fig, ax = plt.subplots()
        labels = {node: f'{node[0:7]}...'
                  if len(node) > 7 else node
                  for node in self.graph.nodes}
        nx.draw_networkx(self.graph, pos=pos, ax=ax, node_size=node_size,
                         node_color=node_color, with_labels=True,
                         node_shape=node_shape,
                         arrowsize=20, font_size=10, font_weight='bold',
                         labels=labels, edge_color='gray')
        ax.set_title(title)
        fig.tight_layout()
        plt.show()
