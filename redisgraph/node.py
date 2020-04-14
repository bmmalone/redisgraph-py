
import pandas as pd
import redisgraph.rg_utils as rg_utils

class Node(object):
    """
    A node within the garph.
    """
    def __init__(self, node_id=None, alias=None, label=None, properties={}):
        """
        Create a new node
        """
        self.id = node_id
        self.alias = alias
        self.label = label
        self.properties = properties

    def toString(self):
        res = ""
        if self.properties:
            props = ','.join(
                key+':'+str(rg_utils.quote_string(val))
                    for key, val in self.properties.items() if pd.notnull(val)
            )
            res += '{' + props + '}'

        return res

    def validate(self):
        """ Validate that the alias, label, and all property names are
        valid identifiers for Cypher

        See the Neo4j docs for more details: https://neo4j.com/docs/cypher-manual/current/syntax/naming/
        """
        rg_utils.validate_cypher_identifier(self.alias)
        rg_utils.validate_cypher_identifier(self.label)

        for k, v in self.properties.items():
            rg_utils.validate_cypher_identifier(k)



    def __str__(self):
        res = '('
        if self.alias:
            res += self.alias
        if self.label:
            res += ':' + self.label
        if self.properties:
            props = ','.join(
                key+':'+str(rg_utils.quote_string(val))
                    for key, val in self.properties.items() if pd.notnull(val)
            )
            res += '{' + props + '}'
        res += ')'

        return res

    def __eq__(self, rhs):
        # Quick positive check, if both IDs are set.
        if self.id is not None and rhs.id is not None and self.id == rhs.id:
            return True

        # Label should match.
        if self.label != rhs.label:
            return False

        # Quick check for number of properties.
        if len(self.properties) != len(rhs.properties):
            return False

        # Compare properties.
        if self.properties != rhs.properties:
            return False

        return True
