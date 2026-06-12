---
name: api-references
description: Expert guidance on matrix-rust-sdk usage, API design, and best practices for the Matrix client-server API
---

# skill

This skill elevates the agent to an expert on the matrix-rust-sdk and the matrix client-server-api. It enables the
agent to guide the developer through conceptional ambiguities where the official documentation falls short.

### When to Use This Skill

Use this skill when the user asks about

 * correct usage of the matrix-rust-sdk and its public API
 * reviewing code using the matrix-rust-sdk API method
 * providing examples or explanation about usage and best practices of the matrix-rust-sdk
 * details on the Matrix client-server API
 * the mapping between the Matrix client-server API and matrix-rust-sdk

### Quick Project Reference

This project, named "gouda", provides a protobuf ABI (defined as a submodule located in `protos/`) for the Matrix
client-server API. The ABI serves as a chat-agnostic abstraction layer and the project implements this
abstraction for the matrix protocol. The project is build on asynchronous Rust utilizing the tokio crate.
Access to the Matrix client-server API is provided through the matrix-rust-sdk.

### Preparation

**Version Numbers**

Before analyzing external references to answer the user prompt, the agent needs to

1. identify the version of the matrix-rust-sdk used currently by this project in the Cargo.toml file.
2. Associate the version number of matrix-rust-sdk with {{ sdk-version }}
3. Fetch https://docs.rs/matrix-sdk/{{ sdk-version }}/matrix_sdk/ruma/api/enum.MatrixVersion.html to obtain the latest supported version of the Matrix client-server API specs (e.g. V1_18 corresponds to v1.18)
4. Associate the version number of Matrix client-server API with {{ matrix-version }}


**Reference code**

If not already present, the agent first needs to clone the code of the matrix-rust-sdk from github to a temporary
location, e.g. /tmp/ using

```
git clone https://github.com/matrix-org/matrix-rust-sdk.git
```

Then check the available tags using

```
git tag --list
```

And check out the tag corresponding the version defined in Cargo.toml

```
git checkout matrix-sdk-{{ sdk-version }}
```

### External References

References crucial for answering Questions related to this skill are:

 * The code base of matrix-rust-sdk cloned to /tmp/matrix-rust-sdk
 * The official documentation of the matrix-rust-sdk can be fetched from https://docs.rs/matrix-sdk/{{ sdk-version }}/matrix_sdk/index.html
 * The documentation of the Matrix client-server API can be fetched from https://spec.matrix.org/{{ matrix-version }}/
 * If needed, use the web-search tool to search for other reference implementations using the matrix-rust-sdk
 * If needed, clone other git-repositories to /tmp and analyze them offline instead of using web-fetch or web-APIs for this task
