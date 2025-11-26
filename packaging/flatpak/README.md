## Building the flatpak

```bash
flatpak run --command=flatpak-builder org.flatpak.Builder build \
    --user \
    --install-deps-from=flathub \
    --disable-rofiles-fuse \
    --force-clean \
    --repo=repo \
    de.gonicus.gonnect.plugins.Matrix.yml
flatpak --user install ./repo de.gonicus.gonnect.plugins.Matrix
```

## Updating supplemental modules

Flatpak builds "offline" only, so we need to extract the Cargo information. To
generate the required modules, you need to have *poetry* and *curl* installed.

Run `./update-cargo-sources` available in this directory.
