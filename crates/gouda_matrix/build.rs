use vergen::{Build, Cargo, Emitter, Rustc};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let build = Build::builder().build_timestamp(true).build();
    let cargo = Cargo::builder()
        .debug(true)
        .opt_level(true)
        .features(true)
        .build();
    let rustc = Rustc::builder().semver(true).channel(true).build();

    Emitter::default()
        .add_instructions(&build)?
        .add_instructions(&cargo)?
        .add_instructions(&rustc)?
        .emit()?;

    Ok(())
}
