# AmiBroker DSE Plugin — v1.2.0

This folder records the upstream v1.2.0 release for the DSE trading system.

- **Upstream repository:** https://github.com/rahathasan452/AmiBroker_DSE_plugin
- **Release page:** https://github.com/rahathasan452/AmiBroker_DSE_plugin/releases/tag/v1.2.0
- **Source tag:** `v1.2.0`
- **Tagged source commit:** `c171ec5723ce01462e729c3f2d27a135a34fa261`
- **Release published:** 2026-10-19

## Release binaries (upstream downloads)

- **64-bit Windows DLL:** https://github.com/rahathasan452/AmiBroker_DSE_plugin/releases/download/v1.2.0/DSE_DataPlugin_x64.dll
- **32-bit Windows DLL:** https://github.com/rahathasan452/AmiBroker_DSE_plugin/releases/download/v1.2.0/DSE_DataPlugin_x86.dll

The DLLs are linked to the original release and are not mirrored in this folder.

## v1.2.0 highlights

- Bounded background backfill worker queue to reduce runaway threads and socket exhaustion during bulk scans.
- Mutex-protected quote cache and thread-local return structures for `GetRecentInfo()`.
- DSE archive request-window clamping to the approximate two-year web archive limit, merging older CSV seed history.
- More resilient HTTP/HTTPS detection and TLS certificate-verification fallback retries.
- Modernized native UI dialog layout and fonts.
- Amarstock index support for `00DSEX` / `DSEX`, `00DS30` / `DS30`, `00DSES` / `DSES`, and `00DSMEX` / `DSMEX`.

## Source layout upstream

The upstream tag contains CMake/build files, configuration, documentation, import formats, C++ headers and source, and scraper utilities. Browse the complete upstream tag here:

https://github.com/rahathasan452/AmiBroker_DSE_plugin/tree/v1.2.0

## Before using it

This is a tracking/manifest file, not a verified installation. Review upstream licensing and build instructions. Build and test in a separate AmiBroker database or staging folder before replacing any plugin used by your live DSE workflow. Verify whether your AmiBroker installation is 32-bit or 64-bit before choosing the matching DLL.
