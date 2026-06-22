import os
import sys

# Ensure the current directory is on the path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from app.service.post_service import serve as serve_grpc

if __name__ == "__main__":
    serve_grpc()