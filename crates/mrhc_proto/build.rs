fn main() {
    let proto_dir = "../../protos";

    println!("cargo:rerun-if-changed={proto_dir}");

    prost_build::Config::new()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile_protos(&["chat.proto"], &[proto_dir])
        .expect("Failed to compile proto files");
}
