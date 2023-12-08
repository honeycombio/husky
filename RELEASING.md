# Release Process

- Update [`version.go`](version.go) with new version
- Add release entry to [changelog](./CHANGELOG.md). Consider using a command like so:
  - `git log --pretty='%C(green)%d%Creset- %s | [%an](https://github.com/)'`
- Commit changes, push, and open a release preparation pull request for review.
- Once the pull request is merged, fetch the updated `main` branch.
- Apply a tag for the new version on the merged commit (e.g. `git tag -a v2.3.1 -m "v2.3.1"`)
- Push the tag upstream (this will kick off the release pipeline in CI) e.g. `git push origin v2.3.1`
- Ensure that there is a draft GitHub release created as part of CI publish steps
- Click "generate release notes" in github for full changelog notes and any new contributors
- Publish the github draft release
