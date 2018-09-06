#!/usr/bin/env python
# -*- coding: utf-8 -*-
import memcache
from zlib import crc32

class HashConsistency(object):

    def __init__(self, nodes=None, replicas=5):
        # 真实节点
        self.nodes = nodes
        # 每个真实节点创建的虚拟节点的个数
        self.replicas = replicas
        # 虚拟节点与真实节点对应关系
        self.nodes_map = []
        # 真实节点与虚拟节点的字典映射(删除节点时)
        self.nodes_replicas = {}

    def _add_nodes_map(self, node):
        """增加虚拟节点到nodes_map列表"""
        nodes_reps = []
        for i in range(self.replicas):
            rep_node = "%s_%s" % (node, i)
            node_hash = abs(crc32(rep_node.encode("utf-8")))
            self.nodes_map.append((node_hash, node))
            nodes_reps.append(node_hash)

        self.nodes_replicas[node] = nodes_reps

    def _sort_nodes(self):
        """按顺序排列虚拟节点"""
        self.nodes_map = sorted(self.nodes_map, key=lambda x: x[0])

    def get_node(self, key):
        """根据key值的hash值，返回对应的节点
        算法：返回最早比key_hash大的节点
        """
        key_hash = abs(crc32(key.encode("utf-8")))
        for node in self.nodes_map:
            if key_hash > node[0]:
                continue
            return node
        return self.nodes_map[0]

    def add_node(self, node):
        """添加节点"""
        self._add_nodes_map(node)
        self._sort_nodes()

    def remove_node(self, node):
        """删除节点"""
        if node not in self.nodes_replicas.keys():
            pass
        discard_rep_nodes = self.nodes_replicas[node]
        self.nodes_map = list(filter(lambda x: x[0] not in discard_rep_nodes, self.nodes_map))


memcache_servers = [

]

h = HashConsistency(memcache_servers)

mc_servers_dict = {}
for ms in memcache_servers:
    h.add_node(ms)
    mc = memcache.Client([ms], debug=0)
    mc_servers_dict[ms] = mc

for k in h.nodes_map:
    print(k)

for i in range(10):
    key = "key_%s" % i
    server = h.get_node(key)[1]
    mc = mc_servers_dict[server]
    mc.set(key, i)
    print("server: %s" % server)
    print(mc)
