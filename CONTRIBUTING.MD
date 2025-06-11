# Local compilation

`mvn clean install [-Pactiveviam] [-Pfast]`

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

    mvn release:clean release:prepare release:perform -Pactiveviam
    
## In case of error:

        git reset --hard HEAD~~
        git tag -d vXXX
        git push --delete origin vXXX

# Coverage

[![Codecov](https://codecov.io/gh/solven-eu/adhoc/graphs/tree.svg?token=GFYB51WRFC)](https://codecov.io/gh/solven-eu/adhoc/)
