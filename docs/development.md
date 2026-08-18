# Development

Planned work lives in the
[issue tracker](https://github.com/home-operations/miroir/issues).

Tooling is pinned with [mise](https://mise.jdx.dev); the everyday
tasks:

```bash
mise run test              # unit tests (regenerates manifests first)
mise run test-integration  # envtest apiserver: CRD schema + CEL rules
mise run test-sanity       # upstream csi-test sanity suite, in-process
mise run lint          # golangci-lint
mise run build         # bin/miroir
mise run manifests     # CRD + RBAC generation
mise run helm-test     # helm-unittest against the chart
mise run docs-serve    # this docs site, live-reloading
```

The end-to-end suite runs against a real Talos cluster booted under QEMU by
[talosctl-cluster-action](https://github.com/home-operations/talosctl-cluster-action);
CI runs it on every PR, and `test/e2e-qemu/README.md` documents how to run it locally.

The docs site is MkDocs Material: pages live under `docs/`, the nav
lives in `mkdocs.yml`, and `mise run docs` builds the deployable
site with `--strict` link checking (CI runs it on every PR).

## Releasing from a fork

The release pipeline is fork-portable: every published name is derived from
the repository that built it, so a fork publishes its own images and its own
chart, and that chart installs those images.

| Artifact                            | Published to                                        |
| ----------------------------------- | --------------------------------------------------- |
| Controller / agent / gateway images | `ghcr.io/<owner>/<repo>-{controller,agent,gateway}` |
| Helm chart (OCI)                    | `ghcr.io/<owner>/charts/miroir`                     |

Pushing to `main` publishes `:main`-tagged images on every commit. A
versioned release is cut by Release Please: merge the release PR it opens,
and the follow-up run tags the repo, publishes the GitHub release, and
builds the semver-tagged images plus the chart — with each image pinned to
the digest that build produced.

Two things are worth knowing before the first release from a fork:

- **The release bot is optional.** Upstream signs release commits with an
  org-level GitHub App (`BOT_CLIENT_ID` / `BOT_APP_PRIVATE_KEY`). Without
  those secrets Release Please falls back to `GITHUB_TOKEN`, which cannot
  raise events that start other workflows — so it dispatches the Release
  workflow at the new tag itself. The same dispatch is the manual
  re-publish button:

  ```bash
  gh workflow run release.yaml --ref 0.11.23
  ```

  One consequence of the fallback: CI does not run on the release PR
  itself, because `GITHUB_TOKEN` opened it. The PR only bumps the version
  and the changelog.

- **GHCR packages start private.** The first release creates them; make the
  three image packages and the chart package public under your account (or
  configure `imagePullSecrets`) before installing.

The `Stale` and `Renovate` workflows are org infrastructure — they dispatch
into `home-operations/.github` — and skip themselves when the bot app is
absent rather than failing on every run. `Pages` still expects GitHub Pages
to be enabled on the repository; disable that workflow if you do not want
the docs site.
