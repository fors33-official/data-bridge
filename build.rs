fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Always re-run if the proto changes (even for slim builds).
    println!("cargo:rerun-if-changed=proto/datastream.proto");

    // Proto + tonic codegen is only needed for the full product (gRPC).
    // Without `full_engine`, the build-dependencies `protoc-bin-vendored` and
    // `tonic-prost-build` are not enabled, so gate codegen to avoid build failures.
    #[cfg(feature = "full_engine")]
    {
        // Use vendored protoc so builds work on Windows and in Docker.
        unsafe {
            std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap());
        }

        tonic_prost_build::configure()
            .compile_protos(&["proto/datastream.proto"], &["proto"])?;
    }

    Ok(())
}

