fn main() {
    let proto_dir = "../../protos";

    println!("cargo:rerun-if-changed={proto_dir}");

    prost_build::compile_protos(&["chat.proto"], &[proto_dir])
        .expect("Failed to compile proto files");
}
