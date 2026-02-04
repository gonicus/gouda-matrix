## Pre-fetch

This seems to be required, as `--ofline` fetch fails elseways.

```bash
cargo fetch --manifest-path ../../Cargo.toml --verbose
```

## Updating supplemental modules

Flatpak builds "offline" only, so we need to extract the Cargo information. To
generate the required modules, you need to have *poetry* and *curl* installed.

Run `./update-cargo-sources` available in this directory.

## Building the flatpak

```bash
flatpak run --command=flatpak-builder org.flatpak.Builder build \
    --user \
    --install-deps-from=flathub \
    --disable-rofiles-fuse \
    --force-clean \
    --repo=repo \
    de.gonicus.gonnect.plugin.Matrix.yml
flatpak --user install ./repo de.gonicus.gonnect.plugin.Matrix
```
