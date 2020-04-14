""" This module contains a wrapper for working with RedisGraph.
"""
import logging
logger = logging.getLogger(__name__)

import tqdm

from .edge import Edge
from .node import Node
from .rg_utils import quote_string, random_string
from .query_result import QueryResult

from typing import Mapping, Optional, Sequence

CREATE_STR = 'CREATE '

class Graph(object):
    """
    Graph, collection of nodes and edges.

    #TODO: create or update nodes based on key (alias): https://stackoverflow.com/questions/25177788/
    """

    def __init__(self, name:str, redis_con, flush_rate:int=100):
        """ Create an in-memory graph
        """
        self.name = name
        self.redis_con = redis_con
        self.flush_rate = flush_rate
        self._initialize()

    def log(self, msg:str, level:int=logging.INFO):    
        """ Log `msg` using `level` using the module-level logger """    
        msg = "[{}] {}".format(self.name, msg)
        logger.log(level, msg)

    def _initialize(self):
        self.recreate_flag = False
        self.nodes = {}
        self.edges = []
        self._labels = []            # List of node labels.
        self._properties = []        # List of properties.
        self._relationshipTypes = [] # List of relation types.
        
        # these objects are "in-memory only", and they will need to be
        # pushed to the database on the next commit
        self._nodes_to_commit = {}

        #TODO: make _edges_to_commit easy to update
        self._edges_to_commit = []


    def get_label(self, idx):
        try:
            label = self._labels[idx]
        except IndexError:
            # Refresh graph labels.
            lbls = self.labels()
            # Unpack data.
            self._labels = [None] * len(lbls)
            for i, l in enumerate(lbls):
                self._labels[i] = l[0]

            label = self._labels[idx]
        return label

    def get_relation(self, idx):
        try:
            relationshipType = self._relationshipTypes[idx]
        except IndexError:
            # Refresh graph relations.
            rels = self.relationship_types()
            # Unpack data.
            self._relationshipTypes = [None] * len(rels)
            for i, r in enumerate(rels):
                self._relationshipTypes[i] = r[0]

            relationshipType = self._relationshipTypes[idx]
        return relationshipType

    def get_property(self, idx):
        try:
            propertie = self._properties[idx]
        except IndexError:
            # Refresh properties.
            props = self.property_keys()
            # Unpack data.
            self._properties = [None] * len(props)
            for i, p in enumerate(props):
                self._properties[i] = p[0]

            propertie = self._properties[idx]
        return propertie

    def add_node(self,
            node:Node,
            alias_property:str=None,
            validate:bool=True) -> str:
        """ Add a node to the graph
        """
        if alias_property is not None:
            node.alias = node.properties[alias_property]

        if node.alias is None:
            #TODO: ensure the alias is unique
            node.alias = random_string()

        if validate:
            node.validate()

        self.nodes[node.alias] = node
        self._nodes_to_commit[node.alias] = node

        return node.alias

    def add_nodes(self,
            node_list:Sequence[Node],
            alias_property:Optional[str]=None,
            validate:bool=True,
            progress_bar:bool=True) -> Sequence[str]:
        """ Add all of the nodes to the graph, optionally setting an alias
        """
        
        it = node_list
        if progress_bar:
            it = tqdm.tqdm(it)

        aliases = [
            self.add_node(
                node,
                alias_property=alias_property,
                validate=validate
            ) for node in it
        ]

        return aliases

    def add_edge(self, edge:Edge, validate_nodes:bool=True) -> None:
        """ Add an edge to the graph
        """

        if validate_nodes:
            # Make sure edge both ends are in the graph
            source_exists = self.nodes.get(edge.src_node.alias) is not None
            dest_exists = self.nodes.get(edge.dest_node.alias) is not None
            assert source_exists and dest_exists

        self.edges.append(edge)
        self._edges_to_commit.append(edge)

    def add_edges(self,
            edge_list:Sequence[Edge],
            validate_nodes:bool=True,
            progress_bar:bool=True) -> None:
        """ Add all of the edges to the graph
        """
        
        it = edge_list
        if progress_bar:
            it = tqdm.tqdm(it)

        for edge in it:
            self.add_edge(edge, validate_nodes=validate_nodes)

    def _commit_list(self, l:Sequence, progress_bar:bool=True):
        if len(l) == 0:
            return None
        
        it = enumerate(l)
        
        if progress_bar:
            it = tqdm.tqdm(it, total=len(l))
        
        q = CREATE_STR
        for i, item in it:
            q += str(item) + ','

            if (i % self.flush_rate) == (self.flush_rate - 1):
                ret = self.query(q)
                q = CREATE_STR

        # commit any remaining items
        if len(q) > len(CREATE_STR):
            self.query(q)
        
    def _commit_nodes(self, progress_bar:bool=True):
        self._commit_list(self._nodes_to_commit.values())
        self._nodes_to_commit = {}
        
    def _commit_edges(self, progress_bar:bool=True):
        self._commit_list(self._edges_to_commit)
        self._edges_to_commit = []

    def recreate_in_database(self):
        if not self.recreate_flag == True:
            msg = ("Please set the `recreate_flag` before calling "
                "`recreate_in_database`")
            self.log(msg, level=logging.WARNING)
            return
        
        self._commit_list(self.nodes.values())
        self._commit_list(self.edges)

        self._nodes_to_commit = {}
        self._edges_to_commit = []

        self.recreate_flag = False
        

    
    def commit(self, progress_bar:bool=True):
        """ Synchronize the graph in redis with the in-memory graph
        """
        self._commit_nodes(progress_bar=progress_bar)
        self._commit_edges(progress_bar=progress_bar)

    def build_params_header(self, params:Mapping) -> str:
        assert type(params) == dict
        # Header starts with "CYPHER"
        params_header = "CYPHER "
        for key, value in params.items():
            # If value is string add quotation marks.
            if type(value) == str:
                value = quote_string(value)
            # Value is None, replace with "null" string.
            elif value is None:
                value = "null"
            params_header += str(key) + "=" + str(value) + " "
        return params_header

    def query(self, query:str, params:Optional[Mapping]=None) -> QueryResult:
        """
        Executes a query against the graph.
        """
        if params is not None:
            query = self.build_params_header(params) + query

        statistics = None
        result_set = None

        # check for simple syntax problems
        # Discard leading comma.
        if query[-1] is ',':
            query = query[:-1]

        response = self.redis_con.execute_command("GRAPH.QUERY", self.name, query, "--compact")
        res = QueryResult(self, response)
        return res

    def _execution_plan_to_string(self, plan) -> str:
        return "\n".join(plan)

    def execution_plan(self, query:str) -> str:
        """ Get the execution plan for given query
        
        GRAPH.EXPLAIN returns an array of operations.
        """
        plan = self.redis_con.execute_command("GRAPH.EXPLAIN", self.name, query)
        return self._execution_plan_to_string(plan)

    def delete(self):
        """ Delete the graph from redis and reset the in-memory graph
        """
        self._initialize()
        ret = self.redis_con.execute_command("GRAPH.DELETE", self.name)
        return ret
    
    def merge(self, pattern) -> QueryResult:
        """ Merge pattern
        """

        query = 'MERGE '
        query += str(pattern)

        return self.query(query)

    # Procedures.
    def call_procedure(self, procedure, *args, **kwagrs) -> QueryResult:
        args = [quote_string(arg) for arg in args]
        q = 'CALL %s(%s)' % (procedure, ','.join(args))

        y = kwagrs.get('y', None)
        if y:
            q += ' YIELD %s' % ','.join(y)

        return self.query(q)

    def labels(self):
        """ Retrieve the set of labels from the database
        """
        return self.call_procedure("db.labels").result_set

    def relationship_types(self):
        """ Retrieve the set of relationships from the database
        """
        return self.call_procedure("db.relationshipTypes").result_set

    def property_keys(self):
        """ Retrieve the set of propertyKeys from the database
        """
        return self.call_procedure("db.propertyKeys").result_set
