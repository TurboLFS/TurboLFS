# TurboLFS

**TurboLFS** is a proof-of-concept high-speed, low-cost, and safe Git LFS proxy — designed as a drop-in replacement for GitHub projects that rely on many LFS-tracked files.

> ⚠️ Currently in early development.

---

## 🚧 Roadmap

Features planned for future releases:

* [ ] Configurable TurboLFS server endpoint (currently hardcoded to `ws://localhost:3000`)
* [ ] Cache promotion
* [ ] Authentication support
* [ ] Progress tracking
* [ ] Performance benchmarks

---

## Installation

1. Download a pre-built binary from the [Releases](https://github.com/TurboLFS/TurboLFS/releases) page.
2. (Optional) Add the executable to your system `PATH` for global access.

---

## Usage

To clone a repository using TurboLFS:

```bash
turbolfs clone https://github.com/<user>/<repo>
```

> Note: This requires the TurboLFS server to be running locally.

Start the local server with:

```bash
./turbolfs turbo server \
  --port 3000 \
  --objects-dir ./<path_to_repo>/.git/lfs/objects \
  --objects-repo <org>/<repo> \
  --object-repo-token <github_token>
```

---

## Building from Source

To build the `turbolfs.exe` binary yourself:

```bash
yarn
npx -y esbuild --bundle --minify --target=node16 --platform=node --outfile=turbolfs_dist.js ./src/index.js
./sea.bat
```

## Curious about TurboLFS or my other projects?
You're welcome to join my personal Discord server for casual discussion around programming and other interests.

Join here: https://discord.gg/puYeyYfvQG

## 🙌 Contributing

We welcome contributions of all kinds — from fixing bugs and improving documentation to developing new features.

To get started, check out our [Contributing Guide](CONTRIBUTING.md).
