#!/usr/bin/env python3
"""
CLI tool for loading circuit tracer graphs into Neo4j.

This script allows you to load graph data from JSON files into a Neo4j database.
It includes functionality to check if graphs are already loaded to avoid duplicates.
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import List, Optional

# Add the circuit_tracer package to the path
sys.path.insert(0, str(Path(__file__).parent.parent))

from circuit_tracer.frontend.neo4j_handler import Neo4jGraphHandler

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def list_graphs(handler: Neo4jGraphHandler) -> List[str]:
    """List all graphs currently loaded in Neo4j."""
    with handler.driver.session() as session:
        result = session.run("""
            MATCH (m:Metadata)
            RETURN m.slug as slug, m.loaded_at as loaded_at, m.file_path as file_path
            ORDER BY m.loaded_at DESC
        """)
        
        graphs = []
        for record in result:
            graphs.append({
                'slug': record['slug'],
                'loaded_at': record['loaded_at'],
                'file_path': record['file_path']
            })
        
        return graphs


def load_single_graph(handler: Neo4jGraphHandler, file_path: str, slug: Optional[str] = None) -> bool:
    """Load a single graph file into Neo4j."""
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return False
    
    # Extract slug from filename if not provided
    if slug is None:
        slug = Path(file_path).stem
    
    try:
        # Check if graph already exists
        metadata = handler.get_metadata(slug)
        if metadata.get('exists', False):
            logger.info(f"Graph '{slug}' already exists in Neo4j (loaded at {metadata.get('loaded_at')})")
            return True
        
        # Load the graph
        logger.info(f"Loading graph '{slug}' from {file_path}")
        handler.load_graph(file_path, slug)
        logger.info(f"Successfully loaded graph '{slug}'")
        return True
        
    except Exception as e:
        logger.error(f"Failed to load graph '{slug}' from {file_path}: {e}")
        return False


def load_directory(handler: Neo4jGraphHandler, directory: str, pattern: str = "*.json") -> int:
    """Load all matching files from a directory."""
    directory_path = Path(directory)
    if not directory_path.exists() or not directory_path.is_dir():
        logger.error(f"Directory not found: {directory}")
        return 0
    
    # Find all matching files
    files = list(directory_path.glob(pattern))
    if not files:
        logger.warning(f"No files matching '{pattern}' found in {directory}")
        return 0
    files = [file for file in files if "metadata" not in file.name]

    logger.info(f"Found {len(files)} files matching '{pattern}' in {directory}")
    
    loaded_count = 0
    for file_path in files:
        if load_single_graph(handler, str(file_path)):
            loaded_count += 1
    
    return loaded_count


def delete_graph(handler: Neo4jGraphHandler, slug: str) -> bool:
    """Delete a graph from Neo4j."""
    try:
        with handler.driver.session() as session:
            # Check if graph exists
            metadata = handler.get_metadata(slug)
            if not metadata.get('exists', False):
                logger.warning(f"Graph '{slug}' not found in Neo4j")
                return False
            
            # Delete nodes and relationships
            logger.info(f"Deleting graph '{slug}' from Neo4j...")
            
            # Delete nodes and their relationships
            session.run("""
                MATCH (n:Node {slug: $slug})
                DETACH DELETE n
            """, {'slug': slug})
            
            # Delete metadata
            session.run("""
                MATCH (m:Metadata {slug: $slug})
                DELETE m
            """, {'slug': slug})
            
            logger.info(f"Successfully deleted graph '{slug}' from Neo4j")
            return True
            
    except Exception as e:
        logger.error(f"Failed to delete graph '{slug}': {e}")
        return False


def clear_all_graphs(handler: Neo4jGraphHandler) -> bool:
    """Clear all graphs from Neo4j."""
    try:
        with handler.driver.session() as session:
            logger.info("Clearing all graphs from Neo4j...")
            
            # Delete all nodes and relationships
            session.run("MATCH (n:Node) DETACH DELETE n")
            
            # Delete all metadata
            session.run("MATCH (m:Metadata) DELETE m")
            
            logger.info("Successfully cleared all graphs from Neo4j")
            return True
            
    except Exception as e:
        logger.error(f"Failed to clear graphs: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Load circuit tracer graphs into Neo4j",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Load a single graph
  python cli_load_graph.py load --file data/my_graph.json

  # Load all JSON files from a directory
  python cli_load_graph.py load --directory data/

  # Load with custom slug
  python cli_load_graph.py load --file data/graph.json --slug my_custom_name

  # List all loaded graphs
  python cli_load_graph.py list

  # Delete a specific graph
  python cli_load_graph.py delete --slug my_graph

  # Clear all graphs
  python cli_load_graph.py clear

  # Custom Neo4j connection
  python cli_load_graph.py load --file data/graph.json --uri bolt://localhost:7687 --user neo4j --password mypassword
        """
    )
    
    # Neo4j connection arguments
    parser.add_argument('--uri', default='bolt://localhost:7687', help='Neo4j URI (default: bolt://localhost:7687)')
    parser.add_argument('--user', default='', help='Neo4j username (default: empty)')
    parser.add_argument('--password', default='', help='Neo4j password (default: empty)')
    
    # Subcommands
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Load command
    load_parser = subparsers.add_parser('load', help='Load graph(s) into Neo4j')
    load_group = load_parser.add_mutually_exclusive_group(required=True)
    load_group.add_argument('--file', help='Single JSON file to load')
    load_group.add_argument('--directory', help='Directory containing JSON files to load')
    load_parser.add_argument('--slug', help='Custom slug for the graph (only with --file)')
    load_parser.add_argument('--pattern', default='*.json', help='File pattern for directory loading (default: *.json)')
    load_parser.add_argument('--force', action='store_true', help='Force reload even if graph exists')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List all loaded graphs')
    
    # Delete command
    delete_parser = subparsers.add_parser('delete', help='Delete a graph from Neo4j')
    delete_parser.add_argument('--slug', required=True, help='Slug of the graph to delete')
    
    # Clear command
    clear_parser = subparsers.add_parser('clear', help='Clear all graphs from Neo4j')
    clear_parser.add_argument('--confirm', action='store_true', help='Confirm deletion of all graphs')
    
    args = parser.parse_args()
    
    if args.password is None:
        logger.error("Neo4j password is required. Use --password argument.")
        return 1
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Connect to Neo4j
    try:
        handler = Neo4jGraphHandler(args.uri, args.user, args.password)
        logger.info(f"Connected to Neo4j at {args.uri}")
    except Exception as e:
        logger.error(f"Failed to connect to Neo4j: {e}")
        return 1
    
    try:
        if args.command == 'load':
            if args.file:
                # Load single file
                success = load_single_graph(handler, args.file, args.slug)
                return 0 if success else 1
            
            elif args.directory:
                # Load directory
                if args.slug:
                    logger.warning("--slug argument ignored when loading directory")
                
                loaded_count = load_directory(handler, args.directory, args.pattern)
                logger.info(f"Loaded {loaded_count} graphs total")
                return 0
        
        elif args.command == 'list':
            graphs = list_graphs(handler)
            if not graphs:
                print("No graphs found in Neo4j")
            else:
                print(f"\nFound {len(graphs)} graph(s) in Neo4j:")
                print("-" * 80)
                for graph in graphs:
                    print(f"Slug: {graph['slug']}")
                    print(f"Loaded: {graph['loaded_at']}")
                    print(f"File: {graph['file_path']}")
                    print("-" * 80)
            return 0
        
        elif args.command == 'delete':
            success = delete_graph(handler, args.slug)
            return 0 if success else 1
        
        elif args.command == 'clear':
            if not args.confirm:
                print("WARNING: This will delete ALL graphs from Neo4j!")
                confirm = input("Type 'yes' to confirm: ")
                if confirm.lower() != 'yes':
                    print("Operation cancelled")
                    return 0
            
            success = clear_all_graphs(handler)
            return 0 if success else 1
        
    finally:
        handler.close()


if __name__ == '__main__':
    sys.exit(main()) 