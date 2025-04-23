from grpc_tools import protoc

protoc.main(
    (
        "",
        "--proto_path=.",
        "--python_out=.",
        "--grpc_python_out=.",
        "--pyi_out=.",
        "route_events/bridge/master/repo/bridge_master.proto",
    )
)
