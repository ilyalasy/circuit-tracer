#!/usr/bin/env python3
"""
CLI script to chunk large graph files for efficient loading.

Usage:
    python chunk_graph_cli.py input_graph.json output_dir --chunk-size 10
"""

import argparse
import logging
import os
import sys
from pathlib import Path

from circuit_tracer.frontend.graph_chunker import chunk_large_graph

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description='Chunk large graph files for efficient loading'
    )
    parser.add_argument(
        '--input_file', 
        default="/Users/ilyalasy/dev/tuwien/circuit-tracer/graphs/gemma-1.json",
        help='Path to the input graph JSON file'
    )
    parser.add_argument(
        '--output_dir',
        default="chunks",
        help='Directory to save chunked files'
    )
    parser.add_argument(
        '--chunk-size', 
        type=float, 
        default=100.0,
        help='Target chunk size in MB (default: 100.0)'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Overwrite existing chunks'
    )
    
    args = parser.parse_args()
    
    # Validate input file
    if not os.path.exists(args.input_file):
        print(f"Error: Input file '{args.input_file}' does not exist")
        sys.exit(1)
    
    # Check file size
    file_size_mb = os.path.getsize(args.input_file) / (1024 * 1024)
    print(f"Input file size: {file_size_mb:.1f} MB")
    
    if file_size_mb < 50:
        print("Warning: File is relatively small, chunking may not be necessary")
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Check if chunks already exist
    input_basename = Path(args.input_file).stem
    metadata_file = os.path.join(args.output_dir, f"{input_basename}_chunks_metadata.json")
    
    if os.path.exists(metadata_file) and not args.force:
        print(f"Chunks already exist for {input_basename}. Use --force to overwrite.")
        sys.exit(1)
    
    # try:
    print(f"Chunking graph with target size {args.chunk_size} MB per chunk...")
    
    metadata = chunk_large_graph(
        args.input_file, 
        args.output_dir, 
        chunk_size_mb=args.chunk_size
    )
    
    print(f"\nChunking completed successfully!")
    print(f"Created {metadata['total_chunks']} chunks")
    print(f"Chunks saved in: {args.output_dir}")
    print(f"Metadata file: {metadata_file}")
    
    # Print chunk summary
    total_size_mb = sum(chunk['size_mb'] for chunk in metadata['chunks'])
    print(f"\nChunk summary:")
    print(f"  Total chunks: {metadata['total_chunks']}")
    print(f"  Total size: {total_size_mb:.1f} MB")
    print(f"  Average chunk size: {total_size_mb / metadata['total_chunks']:.1f} MB")
    
    # Show top 5 most important chunks
    top_chunks = sorted(metadata['chunks'], key=lambda x: x['importance'], reverse=True)[:5]
    print(f"\nTop 5 most important chunks:")
    for i, chunk in enumerate(top_chunks, 1):
        print(f"  {i}. Chunk {chunk['chunk_id']:03d}: "
                f"{chunk['node_count']} nodes, "
                f"{chunk['link_count']} links, "
                f"{chunk['size_mb']:.1f} MB, "
                f"importance: {chunk['importance']:.2f}")
        
    # except Exception as e:
    #     logger.error(f"Chunking failed: {e}")
    #     sys.exit(1)


if __name__ == '__main__':
    main() 