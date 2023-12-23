from uuid import uuid4
from neo4j import GraphDatabase
import ifcopenshell


class Node(dict):
    """
    A node in a graph.
    """

    __primarylabel__ = None
    __primarykey__ = None

    def __init__(self, *args, **kwargs):
        """
        Create a new node.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        super().__init__(*args, **kwargs)
        self.labels = set()

    def add_label(self, label):
        """
        Add a label to the node.

        Args:
            label (str): The label to add.
        """
        self.labels.add(label)

    def __repr__(self):
        """
        Get the string representation of the node.

        Returns:
            str: The string representation of the node.
        """
        return str(self)

    def __str__(self):
        """
        Get the string representation of the node.

        Returns:
            str: The string representation of the node.
        """
        if len(self.items()) == 0:
            return "{}"

        res = "{"
        for key, value in self.items():
            if isinstance(value, (int, float)):
                res += f"{key}: {value}, "
            else:
                res += f'{key}: "{value}", '
        res = res[:-2]
        res += "}"
        return res

    def __hash__(self):
        return hash(self.__primarykey__)

    def __eq__(self, o: object):
        if isinstance(o, Node):
            return self.__hash__() == o.__hash__()
        return False


class Relationship:
    """
    A relationship between two nodes.
    """

    def __init__(self, start_node, rel_type, end_node):
        """
        Create a new relationship.

        Args:
            start_node (Node): The start node.
            rel_type (str): The relationship type.
            end_node (Node): The end node.
        """
        self.start_node = start_node
        self.rel_type = rel_type
        self.end_node = end_node

    def __repr__(self):
        """
        Get the string representation of the relationship.

        Returns:
            str: The string representation of the relationship.
        """
        return str(self)

    def __str__(self):
        """
        Get the string representation of the relationship.

        Returns:
            str: The string representation of the relationship.
        """
        return f"Relationship({self.start_node}, {self.rel_type}, {self.end_node})"

    def __hash__(self):
        return hash((self.start_node, self.rel_type, self.end_node))

    def __eq__(self, o: object):
        if isinstance(o, Relationship):
            return self.__hash__() == o.__hash__()
        return False


class Graph:
    """
    A graph.
    """

    def __init__(self):
        """
        Create a new graph.
        """
        self.nodes = set()
        self.relationships = set()

    def merge(self, entity):
        """
        Merge an entity into the graph.

        Args:
            entity (Node or Relationship): The entity to merge.
        """
        if isinstance(entity, Node):
            self.nodes.add(entity)
        elif isinstance(entity, Relationship):
            self.relationships.add(entity)
        else:
            raise TypeError("Can only merge nodes and relationships")

    def __repr__(self):
        """
        Get the string representation of the graph.

        Returns:
            str: The string representation of the graph.
        """
        return str(self)

    def __str__(self):
        """
        Get the string representation of the graph.

        Returns:
            str: The string representation of the graph.
        """
        return f"Graph({super().__repr__()})"


# Create the basic node with literal attributes and the class hierarchy
def create_pure_node_from_ifc_entity(ifc_entity, ifc_file, hierarchy=True):
    """
    Create a pure node from an IFC entity.

    Args:
        ifc_entity (IFCEntity): The IFC entity to create the node from.
        ifc_file (IFCFile): The IFC file containing the entity.
        hierarchy (bool, optional): Flag indicating whether to include hierarchy labels.
            Defaults to True.

    Returns:
        Node: The created pure node.
    """
    node = Node()
    if ifc_entity.id() != 0:
        node["id"] = str(ifc_entity.id())
    else:
        node["id"] = str(uuid4())
    node["name"] = ifc_entity.is_a()
    if hierarchy:
        for label in ifc_file.wrapped_data.types_with_super():  # TODO: 可能的性能瓶颈
            if ifc_entity.is_a(label):
                node.add_label(label)
    else:
        node.add_label(ifc_entity.is_a())
    attributes_type = ["ENTITY INSTANCE", "AGGREGATE OF ENTITY INSTANCE", "DERIVED"]
    for i in range(ifc_entity.__len__()):
        if not ifc_entity.wrapped_data.get_argument_type(i) in attributes_type:
            name = ifc_entity.wrapped_data.get_argument_name(i)
            name_value = ifc_entity.wrapped_data.get_argument(i)
            node[name] = name_value
    node.__primarylabel__ = node["name"]
    node.__primarykey__ = node["id"]
    return node


# Process literal attributes, entity attributes, and relationship attributes
def create_graph_from_ifc_entity_all(graph, ifc_entity, ifc_file):
    """
    Create a graph representation of the given IFC entity and its related entities.

    Args:
        graph: The graph object to store the entities and relationships.
        ifc_entity: The IFC entity to create the graph from.
        ifc_file: The IFC file containing the entity.

    Returns:
        None
    """
    node = create_pure_node_from_ifc_entity(ifc_entity, ifc_file)
    graph.merge(node)
    for i in range(ifc_entity.__len__()):
        if ifc_entity[i]:
            if ifc_entity.wrapped_data.get_argument_type(i) == "ENTITY INSTANCE":
                if (
                    ifc_entity[i].is_a() in ["IfcOwnerHistory"]
                    and ifc_entity.is_a() != "IfcProject"
                ):
                    continue
                sub_node = create_pure_node_from_ifc_entity(ifc_entity[i], ifc_file)
                rel = Relationship(
                    node, ifc_entity.wrapped_data.get_argument_name(i), sub_node
                )
                graph.merge(rel)
            elif (
                ifc_entity.wrapped_data.get_argument_type(i)
                == "AGGREGATE OF ENTITY INSTANCE"
            ):
                for sub_entity in ifc_entity[i]:
                    sub_node = create_pure_node_from_ifc_entity(sub_entity, ifc_file)
                    rel = Relationship(
                        node, ifc_entity.wrapped_data.get_argument_name(i), sub_node
                    )
                    graph.merge(rel)
    for rel_name in ifc_entity.wrapped_data.get_inverse_attribute_names():
        if ifc_entity.wrapped_data.get_inverse(rel_name):
            inverse_relations = ifc_entity.wrapped_data.get_inverse(rel_name)
            for wrapped_rel_entity in inverse_relations:
                rel_entity = ifc_file.by_id(wrapped_rel_entity.id())
                sub_node = create_pure_node_from_ifc_entity(rel_entity, ifc_file)
                rel = Relationship(node, rel_name, sub_node)
                graph.merge(rel)


def create_full_graph(graph, ifc_file):
    """
    Create a full graph from an IFC file.

    Args:
        graph (Graph): The graph object to store the entities.
        ifc_file (IFCFile): The IFC file object.

    Returns:
        None
    """
    idx = 1
    length = len(ifc_file.wrapped_data.entity_names())
    for entity_id in ifc_file.wrapped_data.entity_names():
        entity = ifc_file.by_id(entity_id)
        print(idx, "/", length, entity)
        create_graph_from_ifc_entity_all(graph, entity, ifc_file)
        idx += 1


def write_graph_to_neo4j(graph, driver, database="neo4j"):
    """
    Write the graph to Neo4j.

    Args:
        graph (Graph): The graph object to store the entities.
        driver (neo4j.Driver): The Neo4j driver.

    Returns:
        None
    """
    for node in graph.nodes:
        query = f"""CREATE (n: {node['name']} {node})"""
        driver.execute_query(query, database=database)

    for rel in graph.relationships:
        start_node = rel.start_node
        end_node = rel.end_node
        query = f"""MATCH (parent: {start_node['name']} {{id: \"{start_node['id']}\"}}),
         (child:{end_node['name']} {{id: \"{end_node['id']}\"}})
                    CREATE (parent)-[:{rel.rel_type}]->(child)"""
        print(query)
        driver.execute_query(query, database=database)


def main():
    """
    This function is the main entry point of the program.
    It opens an ifc_file, connects to Neo4j, creates a new graph,
    creates nodes and relationships in the graph based on the ifc_file data.
    """
    # Open ifc_file
    auth = ("username", "password")
    try:
        ifc_file = ifcopenshell.open("ifc-files/M6.ifc")
        # Connect to Neo4j
        driver = GraphDatabase.driver(
            "bolt://localhost:7687", auth=auth
        )

        # Create a new graph
        driver.execute_query("MATCH (n) DETACH DELETE n")

        # Create nodes
        graph = Graph()
        create_full_graph(graph, ifc_file)
        write_graph_to_neo4j(graph, driver, "ifc-graph")
    except Exception as e:
        print(e)


if __name__ == "__main__":
    import time

    start_time = time.time()
    main()
    end_time = time.time()
    print("Time:", end_time - start_time)
