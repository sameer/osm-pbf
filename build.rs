use std::path::{Path, PathBuf};

use pb_rs::{types::FileDescriptor, ConfigBuilder};

const OSM_PBF_DIR_PATH: &str = "OSM-binary/osmpbf";
const OSM_PBF_FILES: &[&str] = &[
    "OSM-binary/osmpbf/fileformat.proto",
    "OSM-binary/osmpbf/osmformat.proto",
];

fn main() -> std::io::Result<()> {
    for file in OSM_PBF_FILES {
        println!("cargo:rerun-if-changed={file}");
    }

    let in_files = OSM_PBF_FILES.iter().map(PathBuf::from).collect::<Vec<_>>();
    let out_dir = Path::new(&std::env::var("OUT_DIR").unwrap()).join("protos");
    let include_path = PathBuf::from(OSM_PBF_DIR_PATH);

    // Delete all old generated files before re-generating new ones
    if out_dir.exists() {
        std::fs::remove_dir_all(&out_dir).unwrap();
    }
    std::fs::DirBuilder::new().create(&out_dir).unwrap();
    FileDescriptor::run(
        &ConfigBuilder::new(&in_files, None, Some(&out_dir), &[include_path])
            .unwrap()
            .single_module(true)
            .dont_use_cow(true)
            .build(),
    )
    .unwrap();

    Ok(())
}
