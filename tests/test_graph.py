import unittest
from hyrun.job.graph import JobGraph

class DummyJob:
    def __init__(self, hash, status='pending', db_id=None):
        self.hash = hash
        self.status = status
        self.db_id = db_id

class TestJobGraph(unittest.TestCase):
    def setUp(self):
        self.jobs = [DummyJob(f'job{i}') for i in range(3)]
        self.dependencies = [
            (self.jobs[0].hash, self.jobs[1].hash),
            (self.jobs[1].hash, self.jobs[2].hash)
        ]
        self.weights = [
            (self.jobs[0].hash, self.jobs[1].hash, 0.5),
            (self.jobs[1].hash, self.jobs[2].hash, 1.0)
        ]
        self.g = JobGraph(jobs=self.jobs, dependencies=self.dependencies, weights=self.weights)

    def test_nodes_added(self):
        for job in self.jobs:
            self.assertIn(job.hash, self.g.graph.nodes)

    def test_edges_added(self):
        for u, v in self.dependencies:
            self.assertIn((u, v), self.g.graph.edges)

    def test_edge_weights(self):
        for u, v, w in self.weights:
            self.assertEqual(self.g.graph.edges[u, v]['weight'], w)

    def test_copy(self):
        g2 = self.g.__copy__()
        self.assertIsNot(self.g, g2)
        self.assertEqual(set(self.g.graph.nodes), set(g2.graph.nodes))
        self.assertEqual(set(self.g.graph.edges), set(g2.graph.edges))

    def test_remove_node(self):
        node = self.jobs[0].hash
        self.g.remove_node(node)
        self.assertNotIn(node, self.g.graph.nodes)

    def test_remove_edge(self):
        u, v = self.dependencies[0]
        self.g.remove_edge(u, v)
        self.assertNotIn((u, v), self.g.graph.edges)

    def test_topological(self):
        topo = self.g.topological
        self.assertEqual(len(topo), len(self.jobs))
        # topo order should start with jobs[0].hash
        self.assertEqual(topo[0], self.jobs[0].hash)

if __name__ == '__main__':
    unittest.main()