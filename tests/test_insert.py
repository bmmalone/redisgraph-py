""" Test various insertions of nodes into the database
"""
import pytest

import redis
import redisgraph
import redisgraph.rg_utils as rg_utils

from typing import List, Sequence

GRAPH_NAME = 'test_graph'

def get_invalid_hla_alleles() -> List[str]:
    invalid_hla_alleles = [
        'DRB1*04:20',
        'B*39:45',
        'C*12:21',
        'DRB1*09:09',
        'B*37:01',
        'A**0201'
    ]

    return invalid_hla_alleles

def get_valid_hla_alleles() -> List[str]:
    valid_hla_alleles = [
        'DRB1_04_20',
        'B_39_45',
        'C_12_21',
        'DRB1_09_09',
        'B_37_01',
        'A__0201'
    ]

    return valid_hla_alleles

def get_hla_loci() -> List[str]:
    hla_loci = [
        'HLA_A',
        'HLA_B',
        'HLA_C',
        'HLA_DRB1',
        'HLA_DRB3',
        'HLA_DRB4',
        'HLA_DRB5',
    ]

    return hla_loci

def get_redis_con() -> redis.Redis:
    redis_con = redis.Redis(host='localhost', port=6379)
    return redis_con

def get_clean_graph(redis_con) -> redisgraph.Graph:
    graph = redisgraph.Graph(GRAPH_NAME, redis_con)
    
    try:
        graph.delete()
    except redis.ResponseError:
        # then the graph did not exist
        pass

    return graph

@pytest.fixture()
def redis_con() -> redis.Redis:
    return get_redis_con()

@pytest.fixture
def clean_graph() -> redisgraph.Graph:
    redis_con = get_redis_con()
    return get_clean_graph(redis_con)

@pytest.fixture
def hla_loci() -> List[str]:
    return get_hla_loci()

def test_insert_with_underscore(hla_loci, clean_graph):
    # create the nodes
    hla_loci_nodes = [
        redisgraph.Node(label='hla_locus', properties={'locus': hla_locus})
            for hla_locus in hla_loci
    ]

    # add them to the graph
    clean_graph.add_nodes(hla_loci_nodes, alias_property='locus')

    # and check the database
    clean_graph.commit()

    expected_num_nodes = len(hla_loci)
    expected_num_nodes_to_commit = 0

    assert len(clean_graph.nodes) == expected_num_nodes
    assert len(clean_graph._nodes_to_commit) == expected_num_nodes_to_commit

def test_insert_invalid_names(invalid_hla_alleles, clean_graph):

    # create the nodes
    hla_allele_nodes = [
        redisgraph.Node(
            label='hla_allele',
            properties={'allele': hla_allele}
        ) for hla_allele in invalid_hla_alleles
    ]

    # add them to the graph
    with pytest.raises(ValueError):
        clean_graph.add_nodes(
            hla_allele_nodes, alias_property='allele', validate=True
        )

def test_fix_invalid_names(invalid_hla_alleles, valid_hla_alleles):
    fixed_hla_alleles = rg_utils.replace_all_symbols(invalid_hla_alleles)
    assert (fixed_hla_alleles == valid_hla_alleles)

def test_insert_missing_property(clean_graph):
    properties = {
        'allele': "A*02:01",
        'missing_property': None
    }
    node = redisgraph.Node(label='hla_allele', properties=properties)
    clean_graph.add_nodes([node])
    clean_graph.commit()


if __name__ == '__main__':
    hla_loci = get_hla_loci()

    redis_con = get_redis_con()
    clean_graph = get_clean_graph(redis_con)
    test_insert_with_underscore(hla_loci, clean_graph)

    invalid_hla_alleles = get_invalid_hla_alleles()
    test_insert_invalid_names(invalid_hla_alleles, clean_graph)

    valid_hla_alleles = get_valid_hla_alleles()
    test_fix_invalid_names(invalid_hla_alleles, valid_hla_alleles)

    test_insert_missing_property(clean_graph)
