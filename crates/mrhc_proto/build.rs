fn main() {
    let proto_dir = "../../proto";

    println!("cargo:rerun-if-changed={proto_dir}");

    prost_build::compile_protos(&["messages.proto"], &[proto_dir])
        .expect("Failed to compile proto files");
}
