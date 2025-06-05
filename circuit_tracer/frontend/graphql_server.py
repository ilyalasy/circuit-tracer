import json
import os
import logging
from typing import Dict, List, Optional, Any

import strawberry
from strawberry.fastapi import GraphQLRouter
from strawberry.types import Info
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import circuit_tracer.frontend.db as db

logger = logging.getLogger(__name__)

# Store for loaded graph data - cache in memory for better performance
_graph_cache: Dict[str, Dict] = {}

def load_graph_data(slug: str, data_dir: str) -> Optional[Dict]:
    """Load graph data from JSON file, with caching."""
    if slug in _graph_cache:
        return _graph_cache[slug]
    
    graph_path = os.path.join(data_dir, f"{slug}.json")
    if not os.path.exists(graph_path):
        return None
    
    logger.info(f"Loading graph data for {slug}")
    with open(graph_path, 'r') as f:
        data = json.load(f)
    
    # Cache the data for future requests
    _graph_cache[slug] = data
    logger.info(f"Cached graph data for {slug}, {len(data.get('nodes', []))} nodes, {len(data.get('links', []))} links")
    return data

# GraphQL Types
@strawberry.type
class Metadata:
    slug: str
    scan: str
    transcoder_list: List[str]
    prompt_tokens: List[str]
    prompt: str
    node_threshold: Optional[float] = None

@strawberry.type
class QParams:
    pinned_ids: List[str]
    supernodes: List[List[str]]
    link_type: str
    clicked_id: str
    sg_pos: str

@strawberry.type
class Node:
    node_id: str
    feature: int
    layer: str
    ctx_idx: int
    feature_type: str
    token_prob: Optional[float] = 0.0
    is_target_logit: Optional[bool] = False
    run_idx: Optional[int] = 0
    reverse_ctx_idx: Optional[int] = 0
    js_node_id: str
    clerp: Optional[str] = ""
    influence: Optional[float] = None
    activation: Optional[float] = None

@strawberry.type
class Link:
    source: str
    target: str
    weight: float

@strawberry.type
class NodeConnection:
    source_node_id: str
    target_node_id: str
    weight: float

@strawberry.type
class NodeConnections:
    inputs: List[NodeConnection]
    outputs: List[NodeConnection]

@strawberry.type
class GraphData:
    metadata: Metadata
    q_params: QParams
    nodes: List[Node]
    links: List[Link]

@strawberry.type
class NodesPage:
    nodes: List[Node]
    total_count: int
    has_next_page: bool

@strawberry.type
class LinksPage:
    links: List[Link]
    total_count: int
    has_next_page: bool

@strawberry.type
class Query:
    @strawberry.field
    def graph_metadata(self, info: strawberry.Info) -> List[Metadata]:
        """Get metadata for all available graphs."""
        conn = db.get_db_connection()
        all_meta = db.get_all_metadata(conn)
        return [
            Metadata(
                slug=m["slug"],
                scan=m["scan"],
                transcoder_list=m.get("transcoder_list", []),
                prompt_tokens=m.get("prompt_tokens", []),
                prompt=m["prompt"],
                node_threshold=m.get("node_threshold")
            )
            for m in all_meta
        ]

    @strawberry.field
    def graph_basic_info(self, slug: str, info: strawberry.Info) -> Optional[GraphData]:
        """Get basic graph info without nodes/links data for initial loading."""
        conn = db.get_db_connection()
        meta = db.get_metadata(conn, slug)
        if not meta:
            return None
        q_params = db.get_q_params(conn, slug) or {}
        return GraphData(
            metadata=Metadata(
                slug=meta["slug"],
                scan=meta["scan"],
                transcoder_list=meta.get("transcoder_list", []),
                prompt_tokens=meta.get("prompt_tokens", []),
                prompt=meta["prompt"],
                node_threshold=meta.get("node_threshold")
            ),
            q_params=QParams(
                pinned_ids=q_params.get("pinned_ids", []),
                supernodes=q_params.get("supernodes", []),
                link_type=q_params.get("link_type", "both"),
                clicked_id=q_params.get("clicked_id", ""),
                sg_pos=q_params.get("sg_pos", "")
            ),
            nodes=[],
            links=[]
        )

    @strawberry.field
    def nodes(
        self, 
        slug: str,
        info: strawberry.Info,
        offset: Optional[int] = 0,
        limit: Optional[int] = 1000,
        layer_filter: Optional[str] = None,
        feature_type_filter: Optional[str] = None,
        ctx_idx_filter: Optional[int] = None,
        influence_min: Optional[float] = None,
        sort_by_influence: Optional[bool] = True,
        max_nodes: Optional[int] = 2000,  # Hard limit for performance
        node_ids: Optional[List[str]] = None  # Filter by specific node IDs
    ) -> Optional[NodesPage]:
        """Get nodes with pagination and filtering, sorted by influence."""
        conn = db.get_db_connection()
        filters = {}
        if layer_filter:
            filters["layer"] = layer_filter
        if feature_type_filter:
            filters["feature_type"] = feature_type_filter
        if ctx_idx_filter is not None:
            filters["ctx_idx"] = ctx_idx_filter
        nodes = db.get_nodes(
            conn, slug, filters,
            node_ids=node_ids,
            influence_min=influence_min,
            sort_by_influence=sort_by_influence,
            offset=offset,
            limit=limit,
            max_nodes=max_nodes
        )
        total_count = len(nodes)
        paginated_nodes = nodes  # Already paginated in SQL
        return NodesPage(
            nodes=[
                Node(
                    node_id=n["node_id"],
                    feature=n["feature"],
                    layer=n["layer"],
                    ctx_idx=n["ctx_idx"],
                    feature_type=n["feature_type"],
                    token_prob=n.get("token_prob", 0.0),
                    is_target_logit=n.get("is_target_logit", False),
                    run_idx=n.get("run_idx", 0),
                    reverse_ctx_idx=n.get("reverse_ctx_idx", 0),
                    js_node_id=n.get("js_node_id", ""),
                    clerp=n.get("clerp", ""),
                    influence=n.get("influence"),
                    activation=n.get("activation")
                )
                for n in paginated_nodes
            ],
            total_count=total_count,
            has_next_page=offset + limit < total_count
        )

    @strawberry.field
    def links(
        self,
        slug: str,
        info: strawberry.Info,
        offset: Optional[int] = 0,
        limit: Optional[int] = 1000,
        source_filter: Optional[str] = None,
        target_filter: Optional[str] = None,
        weight_min: Optional[float] = None,
        node_ids: Optional[List[str]] = None
    ) -> Optional[LinksPage]:
        """Get links with pagination and filtering."""
        conn = db.get_db_connection()
        filters = {}
        if source_filter:
            filters["source"] = source_filter
        if target_filter:
            filters["target"] = target_filter
        links = db.get_edges(
            conn, slug, filters,
            node_ids=node_ids,
            weight_min=weight_min,
            offset=offset,
            limit=limit
        )
        total_count = len(links)
        paginated_links = links  # Already paginated in SQL
        return LinksPage(
            links=[
                Link(
                    source=l["source"],
                    target=l["target"],
                    weight=l["weight"]
                )
                for l in paginated_links
            ],
            total_count=total_count,
            has_next_page=offset + limit < total_count
        )

    @strawberry.field
    def node_connections(
        self,
        slug: str,
        node_id: str,
        info: strawberry.Info,
        max_inputs: Optional[int] = 50,
        max_outputs: Optional[int] = 50,
        sort_by_weight: Optional[bool] = True,
        weight_min: Optional[float] = None
    ) -> Optional[NodeConnections]:
        """Get input and output connections for a specific node."""
        conn = db.get_db_connection()
        # Inputs: edges where this node is the target
        input_links = db.get_edges(
            conn, slug, {"target": node_id},
            weight_min=weight_min,
            limit=max_inputs
        )
        # Outputs: edges where this node is the source
        output_links = db.get_edges(
            conn, slug, {"source": node_id},
            weight_min=weight_min,
            limit=max_outputs
        )
        if sort_by_weight:
            input_links.sort(key=lambda l: abs(l.get("weight", 0) or 0), reverse=True)
            output_links.sort(key=lambda l: abs(l.get("weight", 0) or 0), reverse=True)
        return NodeConnections(
            inputs=[
                NodeConnection(
                    source_node_id=l["source"],
                    target_node_id=node_id,
                    weight=l["weight"]
                ) for l in input_links
            ],
            outputs=[
                NodeConnection(
                    source_node_id=node_id,
                    target_node_id=l["target"],
                    weight=l["weight"]
                ) for l in output_links
            ]
        )

@strawberry.type
class Mutation:
    @strawberry.field
    def save_graph_params(self, slug: str, q_params: str, info: strawberry.Info) -> bool:
        """Save graph parameters."""
        conn = db.get_db_connection()
        try:
            db.insert_q_params(conn, slug, json.loads(q_params))
            return True
        except Exception as e:
            logger.error(f"Error saving graph parameters for {slug}: {e}")
            return False

# Create schema
schema = strawberry.Schema(query=Query, mutation=Mutation)

def create_graphql_app(data_dir: str, frontend_dir: str) -> FastAPI:
    """Create FastAPI app with GraphQL endpoint."""
    app = FastAPI(title="Circuit Tracer GraphQL API")
    
    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Create GraphQL router with context
    graphql_app = GraphQLRouter(
        schema,
        context_getter=lambda: {"data_dir": data_dir}
    )
    
    # Mount GraphQL endpoint
    app.include_router(graphql_app, prefix="/graphql")
    
    # Serve static frontend files
    app.mount("/", StaticFiles(directory=frontend_dir, html=True), name="static")
    
    @app.get("/health")
    def health_check():
        return {"status": "ok"}
    
    return app 