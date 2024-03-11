#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (c) Antfin, Inc. All rights reserved.
# @Time    : 2024/3/11 11:13
# @Author  : tonymhl
# @Version : Python3
# @File    : json2Neo4j.py
# @Software: PyCharm

from neo4j import GraphDatabase
import json

class KnowledgeGraphBuilder:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_entity_relationship(self, head, relation, tail):
        with self.driver.session() as session:
            result = session.write_transaction(self._create_and_return_relationship, head, relation, tail)
            print(result)

    @staticmethod
    def _create_and_return_relationship(tx, head, relation, tail):
        query = (
            f"MERGE (h:Entity {{name: $head}}) "
            f"MERGE (t:Entity {{name: $tail}}) "
            f"MERGE (h)-[r:{relation}]->(t) "
            f"RETURN h, r, t"
        )
        result = tx.run(query, head=head, tail=tail)
        try:
            return [{"h": record["h"]["name"], "r": relation, "t": record["t"]["name"]}
                    for record in result]
        except Exception as e:
            print("Error creating relationship: ", e)
            return None

# 读取 JSON 文件
with open('/train_example.json', 'r') as file:
    data = json.load(file)

# 连接到 Neo4j 数据库
db_uri = "bolt://localhost:7687"  # Neo4j 默认bolt端口
db_user = "neo4j"  # 替换为您的用户名
db_password = "password"  # 替换为您的密码
kg_builder = KnowledgeGraphBuilder(db_uri, db_user, db_password)

# 创建知识图谱中的节点和关系
for item in data:
    text = item['text']
    triples = item['triples']
    for triple in triples:
        head, relation, tail = triple
        kg_builder.create_entity_relationship(head, relation, tail)

# 关闭数据库连接
kg_builder.close()

