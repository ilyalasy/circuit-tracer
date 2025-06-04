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
                self.wfile.write(
                    f.read().replace(
                        b"window.isLocalServing = false;", b"window.isLocalServing = true;"
                    )
                )
            return

        # Handle data and graph_data requests from local storage
        if self.path.startswith(("/data/", "/graph_data/")):
            # Extract the file path from the URL
            if self.path.startswith("/data/"):
                rel_path = self.path[len("/data/") :].split("?")[0]
            else:  # /graph_data/
                rel_path = self.path[len("/graph_data/") :].split("?")[0]

            # Properly join paths to handle missing slashes
            local_path = os.path.join(self.data_dir, rel_path)

            logger.info(
                f"Rewritten path to {local_path}. "
                f"(self.path: {self.path}; self.data_dir: {self.data_dir})"
            )
            if not os.path.exists(local_path):
                self.send_response(404)
                self.end_headers()
                return

            # Get file size for large file detection
            file_size = os.path.getsize(local_path)
            
            # Check for Range header (HTTP Range Requests)
            range_header = self.headers.get('Range')
            if range_header and file_size > 100 * 1024**2:  # Support ranges for files > 100MB
                logger.info(f"Range request for {local_path}: {range_header}")
                self._handle_range_request(local_path, file_size, range_header)
                return
            
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            
            # For very large files (>500MB), suggest Range requests
            if file_size > 500 * 1024**2:
                self.send_header("Accept-Ranges", "bytes")
                logger.warning(f"Large file {local_path} ({file_size / 1024**2:.2f} MB) - consider using Range requests")
            
            # For large files, use chunked transfer encoding with streaming compression
            if file_size > 10 * 1024**2:  # 10MB threshold for streaming
                logger.info(f"Streaming large file {local_path} ({file_size / 1024**2:.2f} MB)")
                self.send_header("Content-Encoding", "gzip")
                self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()
                
                # Stream compressed data in chunks
                chunk_size = 64 * 1024  # 64KB chunks
                compressor = gzip.GzipFile(fileobj=self.wfile, mode='wb', compresslevel=3)
                
                try:
                    with open(local_path, "rb") as f:
                        total_read = 0
                        while True:
                            chunk = f.read(chunk_size)
                            if not chunk:
                                break
                            
                            # Check if client is still connected before writing
                            try:
                                compressor.write(chunk)
                                # Force a small flush to detect broken pipes earlier
                                if total_read % (10 * 1024**2) == 0:  # Every 10MB
                                    compressor.flush()
                            except (BrokenPipeError, ConnectionResetError, OSError) as e:
                                logger.info(f"Client disconnected after {total_read / 1024**2:.1f} MB: {e}")
                                break
                                
                            total_read += len(chunk)
                            
                            # Log progress for very large files
                            if total_read % (50 * 1024**2) == 0:  # Every 50MB
                                logger.info(f"Streamed {total_read / 1024**2:.1f} MB of {file_size / 1024**2:.1f} MB")
                    
                    compressor.close()
                    logger.info(f"Successfully streamed {local_path} ({total_read / 1024**2:.1f} MB)")
                    
                except (BrokenPipeError, ConnectionResetError, OSError) as e:
                    logger.info(f"Client disconnected during streaming: {e}")
                    try:
                        compressor.close()
                    except:
                        pass
                except Exception as e:
                    logger.error(f"Error streaming file: {e}")
                    try:
                        compressor.close()
                    except:
                        pass
                    
            else:
                # For smaller files, use the original approach
                with open(local_path, "rb") as f:
                    content = f.read()

                # Compress responses over 1MB
                if len(content) > 1024**2:  # 1MB threshold
                    content = gzip.compress(content, compresslevel=3)
                    logger.info(f"Compressed {local_path} to {len(content) / 1024**2:.2f} MB")
                    self.send_header("Content-Encoding", "gzip")

                self.send_header("Content-Length", len(content))
                self.end_headers()
                self.wfile.write(content)
            
            return

        super().do_GET()

    def _handle_range_request(self, local_path, file_size, range_header):
        """Handle HTTP Range requests for large files."""
        try:
            # Parse Range header (e.g., "bytes=0-1023" or "bytes=1024-")
            if not range_header.startswith('bytes='):
                self.send_response(400)
                self.end_headers()
                return
                
            range_spec = range_header[6:]  # Remove "bytes="
            
            if '-' not in range_spec:
                self.send_response(400)
                self.end_headers()
                return
                
            start_str, end_str = range_spec.split('-', 1)
            
            # Parse start and end positions
            start = int(start_str) if start_str else 0
            end = int(end_str) if end_str else file_size - 1
            
            # Validate range
            if start < 0 or start >= file_size or end >= file_size or start > end:
                self.send_response(416)  # Range Not Satisfiable
                self.send_header("Content-Range", f"bytes */{file_size}")
                self.end_headers()
                return
                
            content_length = end - start + 1
            
            logger.info(f"Serving range {start}-{end} ({content_length / 1024**2:.2f} MB) of {local_path}")
            
            self.send_response(206)  # Partial Content
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(content_length))
            self.send_header("Content-Range", f"bytes {start}-{end}/{file_size}")
            self.send_header("Accept-Ranges", "bytes")
            
            # For range requests, don't compress (keeps it simple and predictable)
            self.end_headers()
            
            # Stream the requested range
            chunk_size = 64 * 1024  # 64KB chunks
            with open(local_path, "rb") as f:
                f.seek(start)
                remaining = content_length
                
                while remaining > 0:
                    read_size = min(chunk_size, remaining)
                    chunk = f.read(read_size)
                    if not chunk:
                        break
                        
                    try:
                        self.wfile.write(chunk)
                    except (BrokenPipeError, ConnectionResetError, OSError) as e:
                        logger.info(f"Client disconnected during range request: {e}")
                        break
                        
                    remaining -= len(chunk)
                    
            logger.info(f"Range request completed for {local_path}")
            
        except Exception as e:
            logger.error(f"Error handling range request: {e}")
            self.send_response(500)
            self.end_headers()

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
    def __init__(self, httpd, server_thread):
        self.httpd = httpd
        self.server_thread = server_thread
        self.logs = []
        self._stopped = False  # Initialize the flag here

        # Add a handler to logger that records to self.logs
        self.log_handler = ListHandler(self.logs)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        self.log_handler.setFormatter(formatter)
        logger.addHandler(self.log_handler)

        # Add a FileHandler to write logs to a file
        self.file_handler = logging.FileHandler("server.log")
        self.file_handler.setFormatter(formatter)
        logger.addHandler(self.file_handler)

        # Add a StreamHandler to write logs to the terminal
        self.stream_handler = logging.StreamHandler(sys.stdout)
        self.stream_handler.setFormatter(formatter)
        logger.addHandler(self.stream_handler)

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

        # Force socket close regardless of shutdown success
        try:
            self.httpd.server_close()
        except Exception as e:
            logger.debug(f"Error during server_close: {e}")

        logger.info("Server stopped")

        # Remove our handler when the server stops
        logger.removeHandler(self.log_handler)
        logger.removeHandler(self.file_handler)
        logger.removeHandler(self.stream_handler)

        # Unregister from atexit to avoid duplicate calls
        atexit.unregister(self.stop)

    def get_logs(self):
        """Return the current log messages."""
        return self.logs


def serve(data_dir, frontend_dir=None, port=8032):
    """Start a local HTTP server in a separate thread.

    Args:
        data_dir: Directory for local graph data.
        frontend_dir: Directory containing frontend files. Defaults to DEFAULT_FRONTEND_DIR.
        port: Port to serve on. Defaults to 8032.

    Returns:
        Server object with a stop() method to shut down the server.
    """

    # Use provided directories or defaults
    frontend_dir = Path(frontend_dir).resolve() if frontend_dir else DEFAULT_FRONTEND_DIR

    frontend_dir_path = Path(frontend_dir)
    if not frontend_dir_path.exists() and frontend_dir_path.is_dir():
        raise ValueError(f"Got frontend dir {frontend_dir} but this is not a valid directory")

    logger.info(f"Serving files from: {frontend_dir}")

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
