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

You may also encounter [Spotless issues around Proxy](https://github.com/diffplug/spotless/issues/1658):

# Coding conventions

The style is ensured with Spotless, and various sub-plugins. Just run `mvn spotless:apply`, or implicitly with `mvn install`.

1. Class names with one word should be prefixed with `Adhoc`, like `AdhocQuery`. Else `Query` would be a naming too generic.
2. Class names with multiple words should not be prefixed with `Adhoc`, as we assume 2+ words is enough to be specific to `Adhoc`.
3. Interfaces should be prefixed with `I`, while abstract classes should be prefixed with `A`.

# Make a release

    mvn release:clean release:prepare -Pfast release:perform -Pactiveviam

Note that `-Pfast` on cli applies only to `prepare`, not to `perform`.

## In case of error:

        git reset --hard HEAD~~
        git tag -d vXXX
        git push --delete origin vXXX

# Coverage

[![Codecov](https://codecov.io/gh/solven-eu/adhoc/graphs/tree.svg?token=GFYB51WRFC)](https://codecov.io/gh/solven-eu/adhoc/)


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