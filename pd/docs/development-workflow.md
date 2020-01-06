# Development Workflow


Start by forking the `pd` GitHub repository, make changes in a branch and then send a pull request. 

## Set up your pd GitHub Repository


After forking the [PD upstream](https://github.com/pingcap-incubator/tinykv/pd/fork) source repository to your personal repository. You can set up your personal development environment for PD project.

```sh
$ cd $GOPATH/src/github.com/pingcap
$ git clone < your personal forked pd repo>
$ cd pd
```

## Set git remote as ``upstream``


```sh
$ git remote add upstream https://github.com/pingcap-incubator/tinykv/pd
$ git fetch upstream
$ git merge upstream/master
...
```

## Create your feature branch


Before making code changes, make sure you create a separate branch for them.

```
$ git checkout -b my-feature
```

## Test your changes


After your code changes, make sure that you have:

- Added test cases for the new code.
- Run `make test`.


## Commit changes


After verification, commit your changes. 

```
$ git commit -am 'information about your feature'
```

## Push to the branch


Push your locally committed changes to the remote origin (your fork).

```
$ git push origin my-feature
```

## Create a Pull Request


Pull requests can be created via GitHub. Refer to [this document](https://help.github.com/articles/creating-a-pull-request/) for more details on how to create a pull request.
