#[cfg(feature = "full_engine")]
fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("failed to resolve vendored protoc");
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }
    tonic_prost_build::configure()
        .build_server(false)
        .compile_protos(&["proto/datastream.proto"], &["proto"])
        .expect("failed to compile gRPC proto");
}

#[cfg(not(feature = "full_engine"))]
fn main() {}
