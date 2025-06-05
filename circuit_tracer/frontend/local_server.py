import atexit
import functools
import gzip
import http.server
import json
import logging
import os
import sys
import socketserver
import threading
from importlib.resources import files
from pathlib import Path
import uvicorn
from circuit_tracer.frontend.graphql_server import create_graphql_app
import circuit_tracer.frontend.db as db

logger = logging.getLogger(__name__)
logger.propagate = False

DEFAULT_FRONTEND_DIR = files("circuit_tracer") / "frontend/assets"


class ListHandler(logging.Handler):
    """Handler that appends log records to a list."""

    def __init__(self, log_list):
        super().__init__()
        self.log_list = log_list

    def emit(self, record):
        msg = self.format(record)
        self.log_list.append(msg)


class ReusableTCPServer(socketserver.TCPServer):
    allow_reuse_address = True


# Create handler for serving circuit graph data
class CircuitGraphHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, frontend_dir=None, data_dir=None, **kwargs):
        self.data_dir = data_dir
        super().__init__(*args, directory=str(frontend_dir), **kwargs)

    def log_message(self, format, *args):
        message = format % args
        logger.info(
            "%s - - [%s] %s" % (self.address_string(), self.log_date_time_string(), message)
        )

    def do_GET(self):
        try:
            self._do_GET()
        except Exception as e:
            logger.exception(f"Error handling GET request: {e}")
            self.send_response(500)
            self.end_headers()

    def _do_GET(self):
        # Redirect feature requests to AWS
        logger.info(f"Received request for {self.path}")

        # Handle both explicit index.html requests and root path requests
        if self.path.endswith("index.html") or self.path == "/":
            logger.info("Serving modified index.html")
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            with open(os.path.join(self.directory, "index.html"), "rb") as f:
                content = f.read()
                # Enable local serving mode
                content = content.replace(
                    b"window.isLocalServing = false;", 
                    b"window.isLocalServing = true;"
                )
                self.wfile.write(content)
            return

        # Handle legacy data requests - now redirect to GraphQL metadata
        if self.path.startswith("/data/graph-metadata.json"):
            logger.info("Redirecting metadata request to GraphQL")
            self.send_response(302)
            self.send_header("Location", "/graphql?query={graphMetadata{slug,scan,transcoderList,promptTokens,prompt,nodeThreshold}}")
            self.end_headers()
            return

        # Handle legacy graph_data requests - these should now use GraphQL
        if self.path.startswith("/graph_data/"):
            logger.info("Legacy graph data request - client should use GraphQL")
            self.send_response(410)  # Gone - resource no longer available
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            error_msg = {
                "error": "Graph data now served via GraphQL. Use /graphql endpoint instead.",
                "migration_info": "Frontend should use GraphQL queries for nodes and links data."
            }
            self.wfile.write(json.dumps(error_msg).encode())
            return

        super().do_GET()

    def do_POST(self):
        if not self.path.startswith("/save_graph/"):
            self.send_response(404)
            return

        try:
            # Extract scan and slug from the URL path
            parts = self.path.split("?")[0].strip("/").split("/")
            slug = parts[-1]

            logger.info(f"Saving graph for {slug}")

            # Read the request body
            content_length = int(self.headers["Content-Length"])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode("utf-8"))

            # Generate filename with timestamp
            save_path = os.path.join(self.data_dir, f"{slug}.json")

            # Read the existing file and update it
            with open(save_path, "r") as f:
                graph = json.load(f)
                graph["qParams"] = data["qParams"]

            with open(save_path, "w") as f:
                json.dump(graph, f, indent=2)

            self.send_response(200)
            self.end_headers()
            logger.info(f"Graph saved: {save_path}")

        except Exception as e:
            logger.exception(f"Error saving graph: {e}")
            self.send_response(500)
            self.end_headers()


class Server:
    def __init__(self, httpd, server_thread, graphql_server=None):
        self.httpd = httpd
        self.server_thread = server_thread
        self.graphql_server = graphql_server
        self.logs = []
        self._stopped = False  # Initialize the flag here

        # Add a handler to logger that records to self.logs
        self.log_handler = ListHandler(self.logs)
        self.log_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(self.log_handler)        
        logger.setLevel(logging.INFO)
        # Register shutdown with atexit
        atexit.register(self.stop)

    def stop(self):
        # Check if already stopped to prevent multiple calls
        if self._stopped:
            return
        self._stopped = True

        logger.info("Stopping server...")

        try:
            # First, stop accepting new connections
            self.httpd.socket.close()
        except Exception as e:
            logger.debug(f"Error closing socket: {e}")

        # Then shutdown the server
        shutdown_thread = threading.Thread(target=self.httpd.shutdown)
        shutdown_thread.daemon = True
        shutdown_thread.start()

        # Wait with timeout for threads to complete
        shutdown_thread.join(timeout=5)
        self.server_thread.join(timeout=5)

        # Stop GraphQL server if running
        if self.graphql_server:
            try:
                self.graphql_server.should_exit = True
            except Exception as e:
                logger.debug(f"Error stopping GraphQL server: {e}")

        # Force socket close regardless of shutdown success
        try:
            self.httpd.server_close()
        except Exception as e:
            logger.debug(f"Error during server_close: {e}")

        logger.info("Server stopped")

        # Remove our handler when the server stops
        logger.removeHandler(self.log_handler)

        # Unregister from atexit to avoid duplicate calls
        atexit.unregister(self.stop)

    def get_logs(self):
        """Return the current log messages."""
        return self.logs


def serve(data_dir, frontend_dir=None, port=8032, use_graphql=True):
    """Start a local HTTP server in a separate thread.

    Args:
        data_dir: Directory for local graph data.
        frontend_dir: Directory containing frontend files. Defaults to DEFAULT_FRONTEND_DIR.
        port: Port to serve on. Defaults to 8032.
        use_graphql: Whether to use the new GraphQL server. Defaults to True.

    Returns:
        Server object with a stop() method to shut down the server.
    """
    logger.addHandler(logging.StreamHandler(sys.stdout))

    # Use provided directories or defaults
    frontend_dir = Path(frontend_dir).resolve() if frontend_dir else DEFAULT_FRONTEND_DIR

    frontend_dir_path = Path(frontend_dir)
    if not frontend_dir_path.exists() and frontend_dir_path.is_dir():
        raise ValueError(f"Got frontend dir {frontend_dir} but this is not a valid directory")

    logger.info(f"Serving files from: {frontend_dir}")

    if use_graphql:
        logger.info("Starting GraphQL server mode")
        # Create GraphQL FastAPI app
        app = create_graphql_app(data_dir, str(frontend_dir))
        
        # Run with uvicorn in a thread
        config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="info")
        graphql_server = uvicorn.Server(config)
        
        server_thread = threading.Thread(target=graphql_server.run, daemon=True)
        server_thread.start()
        
        logger.info(f"GraphQL server serving at http://localhost:{port}")
        logger.info(f"GraphQL endpoint: http://localhost:{port}/graphql")
        logger.info(f"Serving files from: {frontend_dir}")
        logger.info(f"Serving data from: {data_dir}")
        
        # Create a dummy httpd for compatibility
        class DummyHttpd:
            def shutdown(self):
                pass
            def server_close(self):
                pass
            @property
            def socket(self):
                class DummySocket:
                    def close(self):
                        pass
                return DummySocket()
        
        return Server(DummyHttpd(), server_thread, graphql_server)
    
    else:
        # Legacy mode - original HTTP server
        logger.info("Starting legacy HTTP server mode")
        # Create a partially applied handler class with configured directories
        handler = functools.partial(CircuitGraphHandler, frontend_dir=frontend_dir, data_dir=data_dir)

        httpd = ReusableTCPServer(("", port), handler)

        # Start the server in a thread
        server_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        server_thread.start()

        logger.info(f"Serving at http://localhost:{port}")
        logger.info(f"Serving files from: {frontend_dir}")
        logger.info(f"Serving data from: {data_dir}")

        return Server(httpd, server_thread)


def main():
    # ... existing code ...
    conn = db.get_db_connection()
    db.init_db(conn)
    # Import all graph JSONs in data_dir if not already present
    data_dir = os.environ.get('GRAPH_DATA_DIR', '../../graphs')
    if not os.path.isabs(data_dir):
        data_dir = os.path.join(os.path.dirname(__file__), data_dir)
    print(f"Importing graphs from {data_dir}")
    if os.path.exists(data_dir):
        db.import_all_graphs_in_dir(conn, data_dir)
        print("Imported all graphs")


if __name__ == "__main__":
    main()
