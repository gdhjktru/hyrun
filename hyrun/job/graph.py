import networkx as nx
from pathlib import Path
from typing import Any, List, Optional, Union, Callable
from hytools.logger import LoggerDummy


# def get_graph(jobs=None,
#               weights=None,
#               dependencies=None,
#               **kwargs):

#     DG = nx.DiGraph()
#     # DG.add_node('root', hash=0)
#     DG.add_nodes_from([tuple([i, {"hash": j.hash}])
#                        for i, j in enumerate(jobs)])
#     # DG.add_edges_from([('root', i) for i in range(len(jobs))])
#     if dependencies:
#         DG.add_edges_from(dependencies)
       
#     # if weights provided, rename adeds
#     if weights:
#         for i, j in enumerate(weights):
#             DG[i][j]['weight'] = weights[i][j]



    

#     return DG


class JobGraph:
    """Class to create a job graph."""

    def __init__(self,
                 jobs=None,
                 dependencies=None,
                 weights=None,
                 logger=None,
                 **kwargs):
        self.logger = logger or LoggerDummy()
        self.graph = nx.DiGraph()
        self.jobs = jobs or []
        self.graph.add_nodes_from([job.hash for job in self.jobs])
        self.update_nodes(self.jobs)
        self.dependencies = self.convert_dependencies(dependencies)
        self.graph.add_edges_from(self.dependencies)
        self.weights = self.convert_weights(weights)
        if self.weights:
            self.graph.add_weighted_edges_from(self.weights)

    def update_nodes(self, jobs=None, key='status'):
        """Add status to nodes."""
        jobs = jobs or self.jobs
        for job in jobs:
            self.graph.nodes[job.hash][key] = getattr(job, key, None)


    def num_to_hash(self, n: Union[str, int]) -> str:
        """Convert number to hash."""
        try:
            hash = str(getattr(self.jobs[int(n)], 'hash', n))
        except (IndexError, ValueError) as e:
            hash = str(n)

        self.logger.debug(f"Converted number {n} to hash: {hash}")
        return hash
        
    def convert_weights(self, weights):
        """Convert weights to graph."""
        if not weights:
            return []
        if isinstance(weights, list):
            return [(self.num_to_hash(u), self.num_to_hash(v), d)
                    for u, v, d in weights]
        else:
            raise ValueError("Weights must be a list or dict.")
        
    def convert_dependencies(self, dependencies):
        """Convert dependencies to graph."""
        if not dependencies:
            return []
        if isinstance(dependencies, list):
            return [(self.num_to_hash(u), self.num_to_hash(v))
                    for u, v in dependencies]
        elif isinstance(dependencies, dict):
            return [(self.num_to_hash(u), self.num_to_hash(v))
                    for u, v in dependencies.items()]
        else:
            raise ValueError("Dependencies must be a list or dict.")

    def subgraph(self, filter_node: Optional[Callable] = None):
        """Create subgraph."""
        if filter_node is None:
            return self.graph
        subgraph = JobGraph()
        subgraph.graph = self.graph.subgraph(
            nx.subgraph_view(self.graph, filter_node=filter_node).nodes())
        return subgraph
    
    def get_running(self):
        """Show running jobs."""
        return self.subgraph(filter_node = (
            lambda n1: self.graph.nodes[n1].get('status', '') == 'running'))
    
    def get_finished(self):
        """Show finished jobs."""
        return self.subgraph(filter_node = (
            lambda n1: self.graph.nodes[n1].get('status', '') == 'finished'))
    
    def get_failed(self):
        """Show failed jobs."""
        return self.subgraph(filter_node = (
            lambda n1: self.graph.nodes[n1].get('status', '') == 'failed'))
    
    def get_pending(self):
        """Show pending jobs."""
        return self.subgraph(filter_node = (
            lambda n1: self.graph.nodes[n1].get('status', '') == 'pending'))
        
    def add_job(self, job):
        """Add job to graph."""
        self.graph.add_node(len(self.graph.nodes))
        
    def add_dependency(self, u, v):
        """Add edge to graph."""
        self.graph.add_edge(u, v)
        
    def remove_job(self, job):
        """Remove job from graph."""
        self.graph.remove_node(job.hash)
        
    def remove_dependency(self, u, v):
        """Remove edge from graph."""
        self.graph.remove_edge(u, v)

    def __iter__(self):
        """Iterate over graph."""
        return iter(self.graph.nodes)

    def __getitem__(self, item):
        """Get item from graph."""
        return self.graph.nodes[item]

    def __len__(self):
        """Get length of graph."""
        return len(self.graph.nodes)

    def __contains__(self, item):
        """Check if item is in graph."""
        return item in self.graph.nodes

    def __str__(self):
        """Get string representation of graph."""
        return str(self.graph.nodes)

    def dump(self, filename=None):
        """Dump graph to file."""
        text = nx.generate_edgelist(self.graph, delimiter=' ', data=True)
        if filename:
            Path(filename).write_text('\n'.join(text))
        else:
            return '\n'.join(text)
    
    def __str__(self) -> str:
        return '\n'.join(nx.generate_network_text(self.graph))
    
    def get_dependencies(self, node):
        """Get all dependencies of a node."""
        return list(nx.ancestors(self.graph, node))
    
    def get_dependents(self, node):
        """Get all dependents of a node."""
        return list(nx.descendants(self.graph, node))
    
    @property
    def nodes(self):
        """Get all nodes in graph."""
        return list(self.graph.nodes)

if __name__ == '__main__':
    from dataclasses import dataclass
    from hytools.logger import get_logger
    @dataclass
    class Job:
        hash: int = 0
        status: str = 'pending'

    jobs = [Job(hash=f'qq{i}fg') for i in range(5, 12)]

    dependencies = [(0, 1), (1, 2), (1, 3), (2, 4)]
    weights = [(0, 1, 1), (1, 2, 2), (1, 3, 3), (2, 4, 4)]
    g = JobGraph(jobs, dependencies=dependencies,
                    weights=weights,
                 logger=get_logger(print_level='DEBUG'))
    jobs[0].status = 'running'
    jobs[1].status = 'running'
    jobs[5].status = 'finished'
    g.update_nodes(jobs)
    print(g.graph.nodes)



    print(g)
    print(g.dump())

    print(list(g.graph.nodes))
    for j in g:
        print(j)

    dep = [g.get_dependencies(j) for j in g]
    print(dep)
    root_jobs = [j for j in g if not g.get_dependencies(j)]
    print('ROOT JOBS without dependencies:', root_jobs)

    # print(g.graph.nodes['qq10fg'])
    # s = g.get_running()
    # print(s)
    # jobs[6].status = 'running'
    # g.update_nodes(jobs)
    # s = g.get_running()
    # print(s)
    # import matplotlib.pyplot as plt
    # plt.tight_layout()
    # nx.draw_networkx(g.graph, arrows=True)
    # plt.axis('off')
    # plt.show()
    # plt.savefig("g2.png", format="PNG")