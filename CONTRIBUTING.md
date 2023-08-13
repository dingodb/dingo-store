# Submission Guidelines

As an open-source-oriented basic storage + computing platform, Dingo-Store should be designed and developed with more attention to detail in every aspect. Based on this principle, the following code submission guidelines are now defined:


# Commit Rules

```shell
[type][module] <description>
```

## 1. Specification parameter description

### 1.1 Rules

- Use [type in lowercase][module in lowercase] with capitalized first letter to describe the issue.
- If multiple modules are designed in a single submission, the final module should be the one with the highest modification weight.
- For proto-related interface submissions, they must be submitted separately.
- Ideally, each commit should only solve one problem, and it is recommended not to submit 100 files in a single commit.

### 1.2 Parameter description



| Parameter                              | Value                                             | Description                                                 |
| :------------------------------------- | :------------------------------------------------ | :---------------------------------------------------------- |
| type (in lowercase)                    | feat                                              | Implementation of new features.                             |
|                                        | fix                                               | Bug fixes.                                                  |
|                                        | doc                                               | Changes related to documentation.                           |
|                                        | style                                             | Code style-related submissions, such as formatting.         |
|                                        | refactor                                          | Refactoring of existing functionality.                      |
|                                        | test                                              | Addition of new test-related code.                          |
|                                        | chore                                             | Integration and deployment-related submissions.             |
| module（in lowercase）                         | coordinator                                       | Metadata management and storage-related.                    |
|                                        | store                                             | Distributed storage-related (e.g. rocksdb, raft-kv-engine). |
|                                        | sdk                                               | Client interface.                                           |
|                                        | deploy                                            | Script-related.                                             |
|                                        | common                                            | Basic modules, such as cmake-related changes.               |
|                                        | proto                                             | Changes related to protobuf.                                |
| description (capitalized first letter) | Description of modifications made in this commit. |                                                             |