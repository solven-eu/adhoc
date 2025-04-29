reference: https://cloud.google.com/bigquery/docs/reference/libraries

# Set up developper environment

```
brew install --cask google-cloud-sdk
```

# Set up authentication

Initialize some defaults (like the projectId):

```
gcloud init
```

then set-up your credentials:

```
gcloud auth application-default login
```

Other useful commands:

```
gcloud auth list
```

# Upload your data

TODO

For Adhoc tests purposes, we rely on [public datasets](https://cloud.google.com/bigquery/public-data)

## GSOD - Daily weather for years:

https://console.cloud.google.com/bigquery?p=bigquery-public-data&d=samples&t=gsod&page=table&project=adhoc-testoverpublicdatasets