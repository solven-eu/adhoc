# Deploying Pivotable

Pivotable is a self-contained Spring Boot application. With the `pivotable-simple_datasets` profile it loads example data in-memory and needs no external database or CDN — any host that can run a Java 25 process and forward one HTTP port will do.

Two recipes live alongside this file; pick the one that matches your constraints.

|            Recipe            | Subdirectory |          Cost           |          Uptime          | Setup time |                                              Notes                                               |
|------------------------------|--------------|-------------------------|--------------------------|------------|--------------------------------------------------------------------------------------------------|
| **Render** (Docker)          | `render/`    | Free tier (web service) | Sleeps after 15 min idle | ~5 min     | Cold-start penalty of ~10-20 s on the first hit; after that, no-ops. Auto-deploys on `git push`. |
| **Oracle Cloud Always-Free** | `oracle/`    | Free forever            | 24/7                     | ~1 h       | ARM Ampere A1-Flex VM + systemd. More ops, no cold starts, permanent URL.                        |

Both recipes run **`pivotable-server-webflux`** with profiles `pivotable-unsafe,pivotable-simple_datasets`:
- `pivotable-unsafe` enables the in-memory fake-user login (no real OAuth2 credentials needed).
- `pivotable-simple_datasets` registers the example cubes (`simple`, `ban`, `films`, `people`, `pixar`, `WorldCupPlayers`).

If you later need real GitHub/Google OAuth2, swap `pivotable-unsafe` for `pivotable-unsafe_external_oauth2` and pass `ADHOC_PIVOTABLE_LOGIN_OAUTH2_GITHUB_CLIENTID` / `CLIENTSECRET` as environment variables — the profile's YAML already declares those placeholders.

## Embedding pivotable in your own service: shipping your own `git.properties`

`pivotable-infra` already declares the [`git-commit-id-maven-plugin`](https://github.com/git-commit-id/git-commit-id-maven-plugin) and bundles its own `classpath:git.properties` in the JAR so `/actuator/info` works out of the box on the shipped binary. `GitPropertySourceConfig` exposes that file as Spring `Environment` properties.

If you build a downstream Spring Boot application on top of `pivotable-infra`, the bundled file is inherited from the Adhoc JAR — meaning `/actuator/info` will report **Adhoc's** commit, not yours. Add the same plugin to your own POM so your build emits its own `classpath:git.properties` that takes precedence on the classpath:

```xml
<plugin>
  <groupId>io.github.git-commit-id</groupId>
  <artifactId>git-commit-id-maven-plugin</artifactId>
  <configuration>
    <failOnNoGitDirectory>false</failOnNoGitDirectory>
    <failOnUnableToExtractRepoInfo>false</failOnUnableToExtractRepoInfo>
  </configuration>
</plugin>
```

The two `false` flags let container builds without a `.git/` directory (Render, Cloud Run, …) finish without failing; in that case the file falls back to placeholder values, which you can override by passing `-Dgit.commit.id.abbrev=$CI_COMMIT_SHA` from CI.
