# Local compilation

`mvn clean install [-Pactiveviam] [-Pfast]`

# Corporate Environment

From a Corporate environment with a strong Proxy, it may be difficult to clone/push code from/to Github.com

One may clone with:

    git clone https://<username>@github.com/solven-eu/adhoc.git

And authenticate with a PAT (with scope=repo) as password:
https://github.com/settings/tokens

You may also encounter Spotless issues around Proxy:

See https://github.com/diffplug/spotless/issues/1658

# Make a release

    mvn release:clean release:prepare release:perform -Pactiveviam
    
## In case of error:

        mvn reset --hard HEAD~~
        git tag -d vXXX
        git push --delete origin vXXX