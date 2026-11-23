import os

import grpc_tools
from grpc_tools import protoc

# inclui os .proto "well-known" (google/protobuf/empty.proto) que vêm com o grpcio-tools
wkt = os.path.join(os.path.dirname(grpc_tools.__file__), "_proto")
rc = protoc.main(["protoc", "-I.", f"-I{wkt}", "--python_out=.", "--grpc_python_out=.",
                  "googol/googol.proto"])
raise SystemExit(rc)
