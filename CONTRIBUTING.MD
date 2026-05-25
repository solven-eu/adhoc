# Local compilation

`mvn clean install [-Pactiveviam] [-Pfast]`

## Maven profiles for local builds

- `-Pfast` — skips both **style checks** (Checkstyle, PMD, CPD, SpotBugs, Spotless, license, formatter, imports, Cleanthat, enforcer) **and tests** (unit + integration + JaCoCo). Use when you only need a quick `package`/`install`.
- `-Pnostyle` — skips the **style checks only**; unit and integration tests still run. Use when you want to compile and run tests without waiting for static analysis (equivalent to passing `-Dcheckstyle.skip -Dpmd.skip -Dspotbugs.skip -Dspotless.skip …` by hand).

Example — compile and run unit tests, skipping style:

    mvn test -Pnostyle --file pom.xml --batch-mode

# Corporate Environment

From a Corporate environment with a strong Proxy, it may be difficult to clone/push code from/to Github.com

One may clone with:

    git clone https://<username>@github.com/solven-eu/adhoc.git

And authenticate with a PAT (with `scope=repo`) as password, generated at https://github.com/settings/tokens

You may also encounter [Spotless issues around Proxy](https://github.com/diffplug/spotless/issues/1658).

Maven JVM options are configured in `.mvn/jvm.config`. In a corporate environment, you may need to add proxy settings there so that Maven (and the JVM it runs in) can reach external repositories:

```
-Dhttp.proxyHost=proxy.example.com
-Dhttp.proxyPort=8080
-Dhttps.proxyHost=proxy.example.com
-Dhttps.proxyPort=8080
-Dhttp.nonProxyHosts=localhost|127.0.0.1|*.example.com
```

Replace the host, port, and non-proxy patterns with the values for your environment. These properties are passed directly to the JVM, so they apply to all HTTP/HTTPS connections made by Maven plugins (including Spotless, Checkstyle, etc.).

## Enterprise / Restricted Execution Environment (IntelliJ + Copilot)

In enterprise environments where OS security policies restrict executable files to specific directories (e.g. `C:\Develop`), the GitHub Copilot IntelliJ plugin may fail to run because it automatically downloads `rg.exe` (Ripgrep) into a user-profile cache folder (e.g. `C:\Users\<user>\.cache\pkg\...`) that is outside the allowed execution zone. This causes the agent features to crash and the plugin to disconnect from GitHub.

**Workaround** ([microsoft/copilot-intellij-feedback#629](https://github.com/microsoft/copilot-intellij-feedback/issues/629)):

1. Download the Ripgrep binary for Windows manually from the [official releases](https://github.com/BurntSushi/ripgrep/releases) and place `rg.exe` inside a folder that your security policy permits execution from (your "sandbox" folder, e.g. `C:\Develop\tools\rg\`).
2. Set the environment variable `GITHUB_COPILOT_RIPGREP_PATH_OVERRIDE` to the full path of that executable:
   ```
   GITHUB_COPILOT_RIPGREP_PATH_OVERRIDE=C:\Develop\tools\rg\rg.exe
   ```
   This can be set as a Windows system/user environment variable, or inside IntelliJ via **Help → Edit Custom VM Options** / `idea.properties`.
3. Restart IntelliJ. Copilot will use the pre-installed binary instead of attempting to download its own.

# Coding conventions

The style is ensured with Spotless, and various sub-plugins. Just run `mvn spotless:apply`, or implicitly with `mvn install`.

1. Class names with one word should be prefixed with `Adhoc`, like `AdhocQuery`. Else `Query` would be a naming too generic.
2. Class names with multiple words should not be prefixed with `Adhoc`, as we assume 2+ words is enough to be specific to `Adhoc`.
3. Interfaces should be prefixed with `I`, while abstract classes should be prefixed with `A`.

# Make a release

    ./scripts/release.sh

The script verifies the preconditions (on `master`, clean tree, in sync with `origin/master`, SNAPSHOT root version, no
pre-existing `vX.Y.Z` tag or `release/vX.Y.Z` branch), creates a dedicated `release/vX.Y.Z` branch off `master`, then
drives the maven-release-plugin from that branch (`-Pfast` for `prepare`, `-Pactiveviam` for `perform`).

`master` is protected, so the script never pushes to it — the release commits and the next-SNAPSHOT bump land on the
`release/vX.Y.Z` branch, which is then opened as a PR for review.

## In case of error:

        git reset --hard HEAD~~
        git tag -d vXXX
        git push --delete origin vXXX

# Coverage

[![Codecov](https://codecov.io/gh/solven-eu/adhoc/graphs/tree.svg?token=GFYB51WRFC)](https://codecov.io/gh/solven-eu/adhoc/)

For the broader context on how Adhoc is tested (cube-level integration vs module unit tests, JMH benchmarks, Playwright e2e), see [docs/testing.md](docs/testing.md).

## Local coverage reports

Each module produces a per-module JaCoCo report at `target/site/jacoco/jacoco.xml` during `mvn package` / `mvn verify`. The per-module report only counts coverage from tests that ran **in that same module**, so a class in `engine/table` exercised exclusively by tests in `engine/cube` will show 0% in `engine/table`'s report even though it is well tested.

To get a faithful, project-wide view, build the **aggregate** report — it merges every module's `jacoco.exec` into a single XML / HTML report:

```
mvn jacoco:report-aggregate -pl :adhoc-aggregate -am
```

Outputs land at `aggregate/target/site/jacoco-aggregate/jacoco.xml` (machine-readable) and `aggregate/target/site/jacoco-aggregate/index.html` (browseable). The aggregate module is a thin pom-only project (`aggregate/pom.xml`) that lists every source module as a `<dependency>` so JaCoCo can locate the per-module exec files; the `-am` (also-make) flag is required so each dependency module joins the reactor before the aggregate goal runs.

## Triaging under-tested classes

`scripts/check-coverage-low.pl` walks the JaCoCo report and flags every class below 20% instruction coverage. It prefers the aggregate report when present, falling back to per-module reports otherwise (with a notice that the picture may be misleading). The 20% threshold is a single constant at the top of the script — bump it to surface higher-coverage gaps. There is also `scripts/check-coverage-sonar.pl` which fetches the same kind of list directly from SonarCloud (no local build required).

### Method-level triage

`scripts/check-coverage-uncovered-methods.pl` reads the project-wide aggregate report and lists every **method** with zero instruction coverage, ordered by LOC (largest first). Useful when class-level triage finds nothing but specific methods inside well-covered classes still slip through:

```
perl scripts/check-coverage-uncovered-methods.pl              # top 50 across the whole project
perl scripts/check-coverage-uncovered-methods.pl 100          # top 100
perl scripts/check-coverage-uncovered-methods.pl 30 'engine/' # top 30 inside engine/* packages
```

Each row reports `LOC`, `Instr` (uncovered instructions), the **owning Maven module** (cross-referenced from each module's per-module report — quick to spot the `experimental` / `pivotable/*` rows you may want to skip), the fully-qualified class, and the method signature. Lombok-generated methods are filtered automatically (via `@lombok.Generated` + JaCoCo's built-in filter, enabled by `lombok.addLombokGeneratedAnnotation = true` in `lombok.config`). The output points at hand-written code only.

# Paperwork

The CLA helps protecting your own contributions, as some corporation may believe `Adhoc` infringes they own copyright.

*Follows [Jackson own policy](https://github.com/FasterXML/jackson/blob/main/CONTRIBUTING.md#paperwork)*

There is not a lot of paperwork related to code changes: Pull Requests are almost all it takes.

But there is one thing that is needed before development team can accept a code change (exception: test code changes do not require one): Contributor License Agreement (CLA). This is needed before your very first code contribution, but covers all Adhoc projects and will NOT be needed for other future contributions.

All you need to do is to download the CLA document, print it, fill and sign, scan (or take photo on your phone) and email that copy to `benoit` at `solven` dot `eu`.

As to document to download, you have 2 choices:

- [Standard Adhoc Contributor License Agreement (CLA)](CONTRIBUTOR-ICLA.MD) is a one-page document we need from every contributor of code (we will request it for pull requests), used mostly by individual contributors. *Follows [Jackson](https://github.com/FasterXML/jackson/tree/main) own [CLA](https://github.com/FasterXML/jackson/blob/main/CLA-jackson-2026.md) as a template.*
- [Corporate CLA](CONTRIBUTOR-CCLA.MD) is used by Corporations to avoid individual employees from having to send separate CLAs; it is also favored by some corporate IP lawyers.
of which the first option is more commonly used (by over 90% of contributors).

Note that the first option is available for corporations as well, but most companies have opted to use the second option instead. Core team has no preference over which one gets used: both work; we care more about actual contributions.