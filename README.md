# repo analyzer

Indexes the full history of a branch into elasticsearch


For full index build:
```
ES="cluster with auth in url" REPO="repo name" BRANCH="branch" MAXAGE="maximum age of commits in years" go run .
```

For incremental index build:
```
# in repo:
git pull
# in here
ES="cluster with auth in url" REPO="repo name" BRANCH="branch" MAXAGE="maximum age of commits in years" INCREMENTAL="true" go run .
```

We delete and re-index the index

Todo:
* Research mitigations for panic in diffing code (sometimes the git library seems to throw up)
