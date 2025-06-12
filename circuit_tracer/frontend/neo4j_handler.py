from neo4j import GraphDatabase
import logging
import json
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

class Neo4jGraphHandler:
    def __init__(self, uri: str, user: str, password: str):
        """Initialize Neo4j connection."""
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
    def close(self):
        """Close the Neo4j connection."""
        self.driver.close()
        
    def load_graph(self, graph_path: str, slug: str):
        """Load graph data into Neo4j using APOC.
        
        Schema:        
        - (n:Node {node_id: string, ...}) - graph nodes        
        - (n1)-[:CONNECTS {weight: float, ...}]->(n2) - node connections
        """
        with self.driver.session() as session:                        
            # Load and create nodes
            session.run("""
                CALL apoc.load.json($file_path)
                YIELD value
                WITH value.nodes as nodes                
                UNWIND nodes as node
                CREATE (n:Node {node_id: node.node_id})
                SET n += {
                    slug: $slug,
                    feature_type: node.feature_type,
                    influence: node.influence,
                    activation: node.activation,
                    properties: node.properties
                }                
            """, {
                'file_path': graph_path,
                'slug': slug
            })
            
            # Create relationships between nodes
            session.run("""
                CALL apoc.periodic.iterate(
                "CALL apoc.load.json('$file_path') YIELD value UNWIND value.links as link RETURN link",
                "MATCH (source:Node {node_id: link.source, slug: $slug})
                MATCH (target:Node {node_id: link.target, slug: $slug})
                CREATE (source)-[r:CONNECTS {weight:link.weight}]->(target)
                ",
                {batchMode: "BATCH", batchSize: 20000, retries:0,parallel:true}
                )
            """, {
                'file_path': graph_path,
                'slug': slug
            })
    
    def get_metadata(self, slug: str) -> Dict[str, Any]:
        """Get graph metadata and counts."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (n:Node {slug: $slug})
                OPTIONAL MATCH (n)-[r:CONNECTS]->()
                RETURN count(n) as node_count, count(r) as link_count
            """, {'slug': slug})
            record = result.single()
            if not record:
                return {}
            
            return {
                'node_count': record['node_count'],
                'link_count': record['link_count']
            }
    
    def get_nodes(self, slug: str, offset: int = 0, limit: int = 1000, sort_by: str = 'influence') -> Dict[str, Any]:
        """Get paginated nodes with optional sorting."""
        with self.driver.session() as session:
            # Get constant nodes (logit and embedding)
            const_nodes = session.run("""
                MATCH (n:Node {slug: $slug})
                WHERE n.feature_type IN ['logit', 'embedding']
                RETURN n
            """, {'slug': slug}).data()
            
            # Get other nodes with pagination
            if sort_by == 'influence':
                query = """
                    MATCH (n:Node {slug: $slug})
                    WHERE NOT n.feature_type IN ['logit', 'embedding']
                    RETURN n
                    ORDER BY abs(n.influence) DESC
                    SKIP $offset
                    LIMIT $limit
                """
            else:
                query = """
                    MATCH (n:Node {slug: $slug})
                    WHERE NOT n.feature_type IN ['logit', 'embedding']
                    RETURN n
                    SKIP $offset
                    LIMIT $limit
                """
            
            other_nodes = session.run(query, {
                'slug': slug,
                'offset': offset,
                'limit': limit
            }).data()
            
            # Get total count
            total_count = session.run("""
                MATCH (n:Node {slug: $slug})
                RETURN count(n) as count
            """, {'slug': slug}).single()['count']
            
            return {
                'nodes': const_nodes + other_nodes,
                'total_count': total_count,
                'offset': offset,
                'limit': limit
            }
    
    def get_links(self, slug: str, node_ids: Optional[List[str]] = None, limit: int = 2000) -> Dict[str, Any]:
        """Get links for specific nodes with optional filtering."""
        with self.driver.session() as session:
            if node_ids:
                query = """
                    MATCH (source:Node {slug: $slug})-[r:CONNECTS]->(target:Node {slug: $slug})
                    WHERE source.node_id IN $node_ids OR target.node_id IN $node_ids
                    RETURN r, source, target
                    ORDER BY abs(r.weight) DESC
                    LIMIT $limit
                """
                result = session.run(query, {
                    'slug': slug,
                    'node_ids': node_ids,
                    'limit': limit
                })
            else:
                query = """
                    MATCH (source:Node {slug: $slug})-[r:CONNECTS]->(target:Node {slug: $slug})
                    RETURN r, source, target
                    ORDER BY abs(r.weight) DESC
                    LIMIT $limit
                """
                result = session.run(query, {
                    'slug': slug,
                    'limit': limit
                })
            
            links = []
            for record in result:
                links.append({
                    'source': record['source']['node_id'],
                    'target': record['target']['node_id'],
                    'weight': record['r']['weight'],
                    'properties': record['r'].get('properties', {})
                })
            
            return {
                'links': links,
                'total_count': len(links)
            }
    
    def get_neighborhood(self, slug: str, center_node_id: str, max_links: int = 20) -> Dict[str, Any]:
        """Get neighborhood for a specific node using apoc.neighbors.athop."""
        with self.driver.session() as session:
            # Get center node and its neighbors
            result = session.run("""
                MATCH ()-[r_in]->(center)
                WHERE center.node_id = $center_id AND center.slug = $slug
                ORDER BY r_in.weight DESC
                LIMIT $max_links
                WITH collect(DISTINCT 
                    { link:r_in, 
                        node: startNode(r_in)
                    }) AS incoming
                RETURN { incoming: incoming } AS result
                UNION
                MATCH (center)-[r_out]->()
                WHERE center.node_id = $center_id AND center.slug = $slug
                ORDER BY r_out.weight DESC
                LIMIT $max_links
                WITH collect(DISTINCT 
                    { link:r_out, 
                        node: endNode(r_out)
                    }) AS outgoing
                RETURN { outgoing: outgoing } AS result
            """, {
                'slug': slug,
                'center_id': center_node_id,
                'max_links': max_links
            })
            
            # Get the two rows from UNION
            records = list(result)
            incoming = records[0]['result']['incoming']
            outgoing = records[1]['result']['outgoing']          
            
            # Extract all nodes and links
            nodes = []            
            links = []
            
            # Process all connections
            for item in incoming + outgoing:
                node = dict(item['node'])
                link = dict(item['link'])
                nodes.append(node)
                links.append({
                    'source': link.get('source'),
                    'target': link.get('target'),
                    'weight': link.get('weight'),
                    'properties': link.get('properties', {})
                })                
            
            return {
                'nodes': list(nodes),
                'links': links,
                'center_node_id': center_node_id
            }