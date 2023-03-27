const OSM_PBF_DIR_PATH: &str = "OSM-binary/osmpbf";
const OSM_PBF_FILES: &[&str] = &[
    "OSM-binary/osmpbf/fileformat.proto",
    "OSM-binary/osmpbf/osmformat.proto",
];

fn main() -> std::io::Result<()> {
    protobuf_codegen::Codegen::new()
        .pure()
        .include(OSM_PBF_DIR_PATH)
        .inputs(OSM_PBF_FILES)
        .cargo_out_dir("protos")
        .run_from_script();
    for file in OSM_PBF_FILES {
        println!("cargo:rerun-if-changed={file}");
    }
    Ok(())
}
