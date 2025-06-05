import sqlite3
from typing import List, Dict, Optional, Any
import json
import os

DB_PATH = "graph.db"

# --- Schema Management ---
def get_db_connection(db_path: str = DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def init_db(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS nodes (
            slug TEXT,
            node_id TEXT,
            feature INTEGER,
            layer TEXT,
            ctx_idx INTEGER,
            feature_type TEXT,
            token_prob REAL,
            is_target_logit BOOLEAN,
            run_idx INTEGER,
            reverse_ctx_idx INTEGER,
            js_node_id TEXT,
            clerp TEXT,
            influence REAL,
            activation REAL,
            PRIMARY KEY (slug, node_id)
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS edges (
            slug TEXT,
            source TEXT,
            target TEXT,
            weight REAL,
            PRIMARY KEY (slug, source, target)
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS metadata (
            slug TEXT PRIMARY KEY,
            scan TEXT,
            transcoder_list TEXT,
            prompt_tokens TEXT,
            prompt TEXT,
            node_threshold REAL
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS q_params (
            slug TEXT PRIMARY KEY,
            pinned_ids TEXT,
            supernodes TEXT,
            link_type TEXT,
            clicked_id TEXT,
            sg_pos TEXT
        )
    ''')
    conn.commit()

# --- Node Operations ---
def insert_node(conn: sqlite3.Connection, slug: str, node: Dict[str, Any]):
    cur = conn.cursor()
    cur.execute('''
        INSERT OR REPLACE INTO nodes (
            slug, node_id, feature, layer, ctx_idx, feature_type, token_prob, is_target_logit, run_idx, reverse_ctx_idx, js_node_id, clerp, influence, activation
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        slug,
        node["node_id"], node["feature"], node["layer"], node["ctx_idx"], node["feature_type"],
        node.get("token_prob", 0.0), node.get("is_target_logit", False), node.get("run_idx", 0),
        node.get("reverse_ctx_idx", 0), node.get("js_node_id", ""), node.get("clerp", ""),
        node.get("influence"), node.get("activation")
    ))
    conn.commit()

def get_nodes(
    conn: sqlite3.Connection,
    slug: str,
    filters: Optional[Dict[str, Any]] = None,
    node_ids: Optional[list] = None,
    influence_min: Optional[float] = None,
    sort_by_influence: Optional[bool] = True,
    offset: int = 0,
    limit: int = 1000,
    max_nodes: Optional[int] = None
) -> List[Dict]:
    cur = conn.cursor()
    query = "SELECT * FROM nodes WHERE slug = ?"
    params = [slug]
    if filters:
        for k, v in filters.items():
            query += f" AND {k} = ?"
            params.append(v)
    if node_ids:
        query += f" AND node_id IN ({','.join(['?']*len(node_ids))})"
        params.extend(node_ids)
    if influence_min is not None:
        query += " AND (influence IS NOT NULL AND influence >= ?)"
        params.append(influence_min)
    if sort_by_influence:
        query += " ORDER BY ABS(COALESCE(influence,0)) DESC"
    else:
        query += " ORDER BY node_id"
    if max_nodes:
        query += f" LIMIT {max_nodes}"
    else:
        query += f" LIMIT {limit} OFFSET {offset}"
    cur.execute(query, params)
    return [dict(row) for row in cur.fetchall()]

# --- Edge Operations ---
def insert_edge(conn: sqlite3.Connection, slug: str, source: str, target: str, weight: float):
    cur = conn.cursor()
    cur.execute('''
        INSERT OR REPLACE INTO edges (slug, source, target, weight) VALUES (?, ?, ?, ?)
    ''', (slug, source, target, weight))
    conn.commit()

def get_edges(
    conn: sqlite3.Connection,
    slug: str,
    filters: Optional[Dict[str, Any]] = None,
    node_ids: Optional[list] = None,
    weight_min: Optional[float] = None,
    offset: int = 0,
    limit: int = 1000
) -> List[Dict]:
    cur = conn.cursor()
    query = "SELECT * FROM edges WHERE slug = ?"
    params = [slug]
    if filters:
        for k, v in filters.items():
            query += f" AND {k} = ?"
            params.append(v)
    if node_ids:
        query += f" AND source IN ({','.join(['?']*len(node_ids))}) AND target IN ({','.join(['?']*len(node_ids))})"
        params.extend(node_ids)
        params.extend(node_ids)
    if weight_min is not None:
        query += " AND (weight IS NOT NULL AND ABS(weight) >= ?)"
        params.append(weight_min)
    query += " ORDER BY ABS(COALESCE(weight,0)) DESC"
    query += f" LIMIT {limit} OFFSET {offset}"
    cur.execute(query, params)
    return [dict(row) for row in cur.fetchall()]

# --- Graph Traversal ---
def get_reachable_nodes(conn: sqlite3.Connection, slug: str, start_node: str, max_depth: int = 10) -> List[str]:
    """Return all nodes reachable from start_node up to max_depth for a given graph."""
    cur = conn.cursor()
    cur.execute(f'''
        WITH RECURSIVE subgraph(node_id, depth) AS (
            SELECT ?, 0
            UNION ALL
            SELECT edges.target, subgraph.depth + 1
            FROM edges JOIN subgraph ON edges.source = subgraph.node_id
            WHERE edges.slug = ? AND subgraph.depth < ?
        )
        SELECT DISTINCT node_id FROM subgraph WHERE node_id != ?
    ''', (start_node, slug, max_depth, start_node))
    return [row[0] for row in cur.fetchall()]

# --- Utility ---
def clear_db(conn: sqlite3.Connection, slug: Optional[str] = None):
    cur = conn.cursor()
    if slug:
        cur.execute('DELETE FROM edges WHERE slug = ?', (slug,))
        cur.execute('DELETE FROM nodes WHERE slug = ?', (slug,))
    else:
        cur.execute('DELETE FROM edges')
        cur.execute('DELETE FROM nodes')
    conn.commit()

# --- Metadata Operations ---
def insert_metadata(conn: sqlite3.Connection, meta: Dict[str, Any]):
    cur = conn.cursor()
    cur.execute('''
        INSERT OR REPLACE INTO metadata (
            slug, scan, transcoder_list, prompt_tokens, prompt, node_threshold
        ) VALUES (?, ?, ?, ?, ?, ?)
    ''', (
        meta["slug"],
        meta["scan"],
        json.dumps(meta.get("transcoder_list", [])),
        json.dumps(meta.get("prompt_tokens", [])),
        meta["prompt"],
        meta.get("node_threshold")
    ))
    conn.commit()

def get_all_metadata(conn: sqlite3.Connection) -> List[Dict]:
    cur = conn.cursor()
    cur.execute('SELECT * FROM metadata')
    rows = cur.fetchall()
    result = []
    for row in rows:
        d = dict(row)
        d["transcoder_list"] = json.loads(d["transcoder_list"] or "[]")
        d["prompt_tokens"] = json.loads(d["prompt_tokens"] or "[]")
        result.append(d)
    return result

def get_metadata(conn: sqlite3.Connection, slug: str) -> Optional[Dict]:
    cur = conn.cursor()
    cur.execute('SELECT * FROM metadata WHERE slug = ?', (slug,))
    row = cur.fetchone()
    if not row:
        return None
    d = dict(row)
    d["transcoder_list"] = json.loads(d["transcoder_list"] or "[]")
    d["prompt_tokens"] = json.loads(d["prompt_tokens"] or "[]")
    return d

# --- QParams Operations ---
def insert_q_params(conn: sqlite3.Connection, slug: str, q_params: Dict[str, Any]):
    cur = conn.cursor()
    cur.execute('''
        INSERT OR REPLACE INTO q_params (
            slug, pinned_ids, supernodes, link_type, clicked_id, sg_pos
        ) VALUES (?, ?, ?, ?, ?, ?)
    ''', (
        slug,
        json.dumps(q_params.get("pinned_ids", [])),
        json.dumps(q_params.get("supernodes", [])),
        q_params.get("link_type", "both"),
        q_params.get("clicked_id", ""),
        q_params.get("sg_pos", "")
    ))
    conn.commit()

def get_q_params(conn: sqlite3.Connection, slug: str) -> Optional[Dict]:
    cur = conn.cursor()
    cur.execute('SELECT * FROM q_params WHERE slug = ?', (slug,))
    row = cur.fetchone()
    if not row:
        return None
    d = dict(row)
    d["pinned_ids"] = json.loads(d["pinned_ids"] or "[]")
    d["supernodes"] = json.loads(d["supernodes"] or "[]")
    return d

# --- Graph Presence Check ---
def graph_exists(conn: sqlite3.Connection, slug: str) -> bool:
    cur = conn.cursor()
    cur.execute('SELECT 1 FROM metadata WHERE slug = ?', (slug,))
    return cur.fetchone() is not None

def import_graph_json(conn: sqlite3.Connection, json_path: str):
    with open(json_path, 'r') as f:
        data = json.load(f)
    # Insert metadata
    meta = data["metadata"]
    slug = meta["slug"]
    insert_metadata(conn, meta)
    # Insert qParams
    q_params = data.get("qParams", {})
    insert_q_params(conn, slug, q_params)
    # Insert nodes
    for node in data.get("nodes", []):
        insert_node(conn, slug, node)
    # Insert links/edges
    for link in data.get("links", []):
        insert_edge(conn, slug, link["source"], link["target"], link["weight"])

def import_all_graphs_in_dir(conn: sqlite3.Connection, data_dir: str):
    for fname in os.listdir(data_dir):
        if fname.endswith('.json') and fname != 'graph-metadata.json':
            slug = fname[:-5]
            if not graph_exists(conn, slug):
                import_graph_json(conn, os.path.join(data_dir, fname)) 