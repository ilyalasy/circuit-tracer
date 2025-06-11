import json
import logging
import os
from collections import defaultdict, deque
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
import heapq
import hashlib
from tqdm import tqdm

try:
    import networkx as nx
    import numpy as np
except ImportError:
    raise ImportError("Please install: pip install networkx numpy")

logger = logging.getLogger(__name__)


class GraphChunker:
    """Efficient graph chunking based on important paths from embeddings to logits."""
    
    def __init__(self, chunk_size_mb: float = 10.0, max_chunks_memory: int = 5):
        self.chunk_size_mb = chunk_size_mb
        self.max_chunks_memory = max_chunks_memory
        self.chunk_size_bytes = int(chunk_size_mb * 1024 * 1024)
        
    def chunk_graph(self, graph_data: dict, output_dir: str, slug: str) -> dict:
        """
        Main chunking function that creates importance-based graph chunks.
        
        Returns metadata about chunks for frontend consumption.
        """
        logger.info(f"Starting graph chunking for {slug}")
        
        # Parse graph structure
        nodes_by_id = {node['node_id']: node for node in graph_data['nodes']}
        links = graph_data['links']
        
        # Build NetworkX graph for path finding
        G = self._build_networkx_graph(nodes_by_id, links)
        
        # Find embedding and logit nodes
        embedding_nodes = self._find_embedding_nodes(nodes_by_id)
        logit_nodes = self._find_logit_nodes(nodes_by_id)
        
        # Find important paths
        important_paths = self._find_important_paths(G, embedding_nodes, logit_nodes)
        
        # Create chunks based on path importance
        chunks = self._create_chunks(important_paths, nodes_by_id, links, graph_data)
        
        # Save chunks to files
        chunk_metadata = self._save_chunks(chunks, output_dir, slug)
        
        logger.info(f"Created {len(chunks)} chunks for {slug}")
        return chunk_metadata
    
    def _build_networkx_graph(self, nodes_by_id: dict, links: list) -> nx.DiGraph:
        """Build NetworkX directed graph from graph data."""
        G = nx.DiGraph()
        
        # Add nodes
        for node_id, node in nodes_by_id.items():
            G.add_node(node_id, **node)
        
        # Add edges with weights
        for link in links:
            G.add_edge(link['source'], link['target'], weight=link.get('weight', 0))
        
        return G
    
    def _find_embedding_nodes(self, nodes_by_id: dict) -> List[dict]:
        """Find embedding nodes, prioritize latest tokens."""
        embedding_nodes = []
        
        for node in nodes_by_id.values():
            if node.get('feature_type') == 'embedding':
                embedding_nodes.append(node)
        return embedding_nodes
    
    def _find_logit_nodes(self, nodes_by_id: dict) -> List[dict]:
        """Find logit nodes, prioritize by token probability."""
        logit_nodes = []
        
        for node in nodes_by_id.values():
            if node.get('feature_type') == 'logit':
                logit_nodes.append(node)
        return logit_nodes
    
    def _find_important_paths(self, G: nx.DiGraph, embedding_nodes: List[dict], 
                            logit_nodes: List[dict]) -> List[dict]:
        """Find all important paths using comprehensive backward traversal from target nodes."""
        important_paths = []
        covered_nodes = set()
        all_nodes = set(G.nodes())
        
        # Create set of predefined source nodes (embeddings)
        source_node_ids = {node["node_id"] for node in embedding_nodes}
        
        # Sort logits by importance (highest probability first)
        sorted_logits = sorted(logit_nodes, key=lambda x: x["token_prob"], reverse=True)[:1]
        
        logger.info(f"Finding comprehensive paths from {len(sorted_logits)} target logits")
        logger.info(f"Total nodes to cover: {len(all_nodes)}")
        
        path_id_counter = 0
        
        for logit in sorted_logits:
            logit_id = logit["node_id"]
            logit_prob = logit["token_prob"]
            
            logger.debug(f"Processing target node: {logit_id} (prob: {logit_prob:.4f})")
            
            # Generate all paths from this target using the comprehensive backward traversal
            for path_nodes in self._backward_traversal_generator(G, logit_id, source_node_ids):
                if len(path_nodes) < 2:
                    continue
                
                # Calculate path metrics
                path_weight = nx.path_weight(G, path_nodes, weight="weight")
              
                # Track which nodes this path covers
                path_coverage = set(path_nodes)
                new_nodes_covered = path_coverage - covered_nodes                
                
                important_paths.append({
                    'path': list(reversed(path_nodes)),  # Reverse to go from embedding to logit
                    'path_weight': path_weight,
                    'logit_prob': logit_prob,
                    'path_id': f"path_{path_id_counter:04d}_{logit_id}",
                    'target_node': logit_id,
                    'nodes_covered': path_coverage,
                    'new_nodes_covered': new_nodes_covered
                })
                
                # Update covered nodes
                covered_nodes.update(path_coverage)
                path_id_counter += 1
                
                # Log progress
                if path_id_counter % 100 == 0:
                    coverage_pct = len(covered_nodes) / len(all_nodes) * 100
                    logger.info(f"Generated {path_id_counter} paths, covered {len(covered_nodes)}/{len(all_nodes)} nodes ({coverage_pct:.1f}%)")
                
                # Stop if we've covered most of the graph
                if len(covered_nodes) >= 0.95 * len(all_nodes):
                    logger.info(f"Reached 95% node coverage, stopping early")
                    break
            
            # Stop processing more targets if we have good coverage
            if len(covered_nodes) >= 0.95 * len(all_nodes):
                break
        
        # Sort by importance
        important_paths.sort(key=lambda x: x['path_weight'], reverse=True)
        
        coverage_pct = len(covered_nodes) / len(all_nodes) * 100
        logger.info(f"Found {len(important_paths)} paths covering {len(covered_nodes)}/{len(all_nodes)} nodes ({coverage_pct:.1f}%)")
        
        return important_paths
    
    def _backward_traversal_generator(self, G: nx.DiGraph, start_node: str, 
                                    predefined_nodes: Set[str], max_depth: int = 10):
        """
        Generator for comprehensive backward traversal algorithm:
        1. Start from target node
        2. Sort all incoming edges by weight (descending)
        3. For each edge, traverse backward following best edges until embedding
        4. Yield complete path
        5. Continue with next best edge from original target
        6. Repeat until all nodes are covered
        """
        # Track all processed starting points to avoid duplicate work
        processed_edges = set()
        
        # Get all incoming edges to the start node, sorted by weight
        initial_edges = list(G.in_edges(start_node, data=True))
        initial_edges.sort(key=lambda x: abs(x[2].get('weight', 0)), reverse=True)
        
        logger.debug(f"Starting backward traversal from {start_node} with {len(initial_edges)} incoming edges")
        
        # Process each incoming edge in order of importance
        for edge_idx, (source_node, target_node, edge_data) in enumerate(initial_edges):
            edge_weight = edge_data.get('weight', 0)
            edge_key = (source_node, target_node)
            
            if edge_key in processed_edges:
                continue
                
            processed_edges.add(edge_key)
            
            logger.debug(f"Processing edge {edge_idx + 1}/{len(initial_edges)}: {source_node} -> {target_node} (weight: {edge_weight:.4f})")
            
            # Start a path from this edge
            path = self._trace_single_path(G, start_node, source_node, predefined_nodes, max_depth)
            
            if path and len(path) >= 2:
                yield path
                
                # Also explore any alternative paths from intermediate nodes in this path
                for i, intermediate_node in enumerate(path[1:-1], 1):  # Skip start and end
                    if intermediate_node == start_node:
                        continue
                        
                    # Find alternative incoming edges from this intermediate node
                    alt_edges = list(G.in_edges(intermediate_node, data=True))
                    alt_edges.sort(key=lambda x: abs(x[2].get('weight', 0)), reverse=True)
                    
                    # Try a few alternative paths (not all to avoid explosion)
                    for alt_source, alt_target, alt_edge_data in alt_edges[:3]:
                        alt_edge_key = (alt_source, alt_target)
                        
                        if alt_edge_key in processed_edges or alt_source in path[:i+1]:
                            continue
                            
                        processed_edges.add(alt_edge_key)
                        
                        # Create alternative path: start -> ... -> intermediate -> alt_source -> ... -> embedding
                        prefix_path = path[:i+1]  # Path from start to intermediate
                        suffix_path = self._trace_single_path(G, intermediate_node, alt_source, predefined_nodes, max_depth - i)
                        
                        if suffix_path and len(suffix_path) >= 2:
                            # Combine paths, avoiding duplication of intermediate_node
                            combined_path = prefix_path + suffix_path[1:]
                            if len(combined_path) >= 2:
                                yield combined_path
    
    def _trace_single_path(self, G: nx.DiGraph, start_node: str, current_node: str,
                          predefined_nodes: Set[str], max_depth: int) -> List[str]:
        """
        Trace a single path backward from current_node following the biggest edges.
        """
        path = [start_node, current_node] if current_node != start_node else [start_node]
        visited = set(path)
        
        for depth in range(max_depth):
            # Check if we've reached a predefined node (embedding)
            if current_node in predefined_nodes:
                break
                
            # Find all incoming edges to current node
            incoming_edges = list(G.in_edges(current_node, data=True))
            
            if not incoming_edges:
                break
            
            # Find the biggest edge that doesn't create a cycle
            best_edge = None
            for source_node, target_node, edge_data in sorted(incoming_edges, 
                                                            key=lambda x: abs(x[2].get('weight', 0)), 
                                                            reverse=True):
                if source_node not in visited:
                    best_edge = (source_node, target_node, edge_data)
                    break
            
            if not best_edge:
                # No cycle-free edges available
                break
            
            source_node, _, edge_data = best_edge
            
            # Move to the source of the biggest edge
            path.append(source_node)
            visited.add(source_node)
            current_node = source_node
            
            logger.debug(f"  Trace step {depth + 1}: -> {current_node} (weight: {edge_data.get('weight', 0):.4f})")
        
        return path
  
    
    def _create_chunks(self, important_paths: List[dict], nodes_by_id: dict, 
                      links: list, graph_data: dict) -> List[dict]:
        """Create graph chunks based on important paths."""
        chunks = []
        used_nodes = set()
        used_links = set()
        
        # Create chunks greedily by importance
        current_chunk_nodes = set()
        current_chunk_links = set()
        current_chunk_importance = 0.0
        current_size_estimate = 0
        
        # Start with metadata that's always in first chunk
        base_size = len(json.dumps({
            'metadata': graph_data.get('metadata', {}),
            'qParams': graph_data.get('qParams', {})
        }).encode())
        
        for path_info in important_paths:
            path_nodes = set(path_info['path'])
            path_links = self._get_path_links(path_info['path'], links)
            
            # Estimate size increase
            new_nodes = path_nodes - current_chunk_nodes
            new_links = path_links - current_chunk_links
            
            size_increase = self._estimate_size_increase(new_nodes, new_links, nodes_by_id, links)
            
            # Check if we should start a new chunk
            if (current_size_estimate + size_increase > self.chunk_size_bytes and 
                current_chunk_nodes):
                
                # Finalize current chunk
                chunk = self._finalize_chunk(
                    current_chunk_nodes, current_chunk_links, 
                    current_chunk_importance, nodes_by_id, links, graph_data
                )
                chunks.append(chunk)
                
                # Start new chunk
                current_chunk_nodes = path_nodes.copy()
                current_chunk_links = path_links.copy()
                current_chunk_importance = path_info['path_weight']
                current_size_estimate = base_size + size_increase
            else:
                # Add to current chunk
                current_chunk_nodes.update(path_nodes)
                current_chunk_links.update(path_links)
                current_chunk_importance += path_info['path_weight']
                current_size_estimate += size_increase
            
            used_nodes.update(path_nodes)
            used_links.update(path_links)
        
        # Finalize last chunk
        if current_chunk_nodes:
            chunk = self._finalize_chunk(
                current_chunk_nodes, current_chunk_links,
                current_chunk_importance, nodes_by_id, links, graph_data
            )
            chunks.append(chunk)
        
        # Create final chunk with remaining nodes/links
        remaining_nodes = set(nodes_by_id.keys()) - used_nodes
        remaining_links = set(range(len(links))) - used_links
        
        if remaining_nodes or remaining_links:
            remaining_chunk = self._finalize_chunk(
                remaining_nodes, remaining_links, 0.0,
                nodes_by_id, links, graph_data
            )
            chunks.append(remaining_chunk)
        
        return chunks
    
    def _get_path_links(self, path: List[str], links: list) -> Set[int]:
        """Find link indices that connect consecutive nodes in path."""
        path_links = set()
        
        for i in range(len(path) - 1):
            source = path[i]
            target = path[i + 1]
            
            for link_idx, link in enumerate(links):
                if link['source'] == source and link['target'] == target:
                    path_links.add(link_idx)
                    break
        
        return path_links
    
    def _estimate_size_increase(self, new_nodes: Set[str], new_links: Set[int],
                              nodes_by_id: dict, links: list) -> int:
        """Estimate size increase in bytes."""
        nodes_size = sum(len(json.dumps(nodes_by_id[node_id]).encode()) 
                        for node_id in new_nodes if node_id in nodes_by_id)
        links_size = sum(len(json.dumps(links[link_idx]).encode()) 
                        for link_idx in new_links if link_idx < len(links))
        return nodes_size + links_size
    
    def _finalize_chunk(self, chunk_nodes: Set[str], chunk_links: Set[int],
                       importance: float, nodes_by_id: dict, links: list,
                       graph_data: dict) -> dict:
        """Create final chunk data structure."""
        chunk_nodes_data = [nodes_by_id[node_id] for node_id in chunk_nodes 
                           if node_id in nodes_by_id]
        chunk_links_data = [links[link_idx] for link_idx in chunk_links 
                           if link_idx < len(links)]
        
        return {
            'metadata': graph_data.get('metadata', {}),
            'qParams': graph_data.get('qParams', {}),
            'nodes': chunk_nodes_data,
            'links': chunk_links_data,
            'importance': importance,
            'node_count': len(chunk_nodes_data),
            'link_count': len(chunk_links_data)
        }
    
    def _save_chunks(self, chunks: List[dict], output_dir: str, slug: str) -> dict:
        """Save chunks to files and return metadata."""
        os.makedirs(output_dir, exist_ok=True)
        
        chunk_metadata = {
            'slug': slug,
            'total_chunks': len(chunks),
            'chunks': []
        }
        
        for i, chunk in enumerate(chunks):
            chunk_filename = f"{slug}_chunk_{i:03d}.json"
            chunk_path = os.path.join(output_dir, chunk_filename)
            
            with open(chunk_path, 'w') as f:
                json.dump(chunk, f, separators=(',', ':'))
            
            file_size = os.path.getsize(chunk_path)
            
            chunk_info = {
                'chunk_id': i,
                'filename': chunk_filename,
                'importance': chunk.get('importance', 0.0),
                'node_count': chunk.get('node_count', 0),
                'link_count': chunk.get('link_count', 0),
                'size_bytes': file_size,
                'size_mb': file_size / (1024 * 1024)
            }
            
            chunk_metadata['chunks'].append(chunk_info)
        
        # Sort chunks by importance
        chunk_metadata['chunks'].sort(key=lambda x: x['importance'], reverse=True)
        
        # Save chunk metadata
        metadata_path = os.path.join(output_dir, f"{slug}_chunks_metadata.json")
        with open(metadata_path, 'w') as f:
            json.dump(chunk_metadata, f, indent=2)
        
        logger.info(f"Saved {len(chunks)} chunks for {slug}")
        return chunk_metadata


def chunk_large_graph(graph_path: str, output_dir: str, chunk_size_mb: float = 10.0) -> dict:
    """Convenience function to chunk a large graph file."""
    with open(graph_path, 'r') as f:
        graph_data = json.load(f)
    
    slug = Path(graph_path).stem
    chunker = GraphChunker(chunk_size_mb=chunk_size_mb)
    
    return chunker.chunk_graph(graph_data, output_dir, slug) 