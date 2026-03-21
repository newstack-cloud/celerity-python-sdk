# Changelog

## [0.2.0](https://github.com/newstack-cloud/celerity-python-sdk/compare/v0.1.0...v0.2.0) (2026-03-21)


### Features

* add bucket resource implementation ([93ba36f](https://github.com/newstack-cloud/celerity-python-sdk/commit/93ba36f50e069ee85c128cb5220dbb46b069fa10))
* add cache resource implementation ([ddf8279](https://github.com/newstack-cloud/celerity-python-sdk/commit/ddf82792c2595971a9c3889726a2efb2d7e44010))
* add datastore implementation with dynamodb provider ([1890a8a](https://github.com/newstack-cloud/celerity-python-sdk/commit/1890a8a195ba823c5085b90bd311202d063f15b4))
* add decorators for all supported features ([1102fd1](https://github.com/newstack-cloud/celerity-python-sdk/commit/1102fd1248017d7a381bfc7646effc752a25ca81))
* add foundation types, metadata system and common utils ([6b49108](https://github.com/newstack-cloud/celerity-python-sdk/commit/6b491086ba6e400670712331b89ea1dcc4866d3c))
* add handler infra with registry, scanners, layers, pipelines and bootstrap ([529ca7c](https://github.com/newstack-cloud/celerity-python-sdk/commit/529ca7c3395eea3a5262ad932b8f949ec551fe0a))
* add handler manifest extraction cli ([f1fed1f](https://github.com/newstack-cloud/celerity-python-sdk/commit/f1fed1fe1123cae4a6b0fa62ddd0726d8593d55b))
* add instrumentation for dynamodb datastore implementation ([65ab6a5](https://github.com/newstack-cloud/celerity-python-sdk/commit/65ab6a5747efb4f88a238eb36b05602b9918ec95))
* add instrumentation to redis cache implementation ([99501a4](https://github.com/newstack-cloud/celerity-python-sdk/commit/99501a411b75463af0590d7d5fb81792ff76dc04))
* add queue resource implementation ([0101e6e](https://github.com/newstack-cloud/celerity-python-sdk/commit/0101e6ea55892738bd967c4ba1a8ff4e83912425))
* add resource type foundations and config implementation ([bb5b0c4](https://github.com/newstack-cloud/celerity-python-sdk/commit/bb5b0c473732f0079fbc61abcc0dae00b68e29e8))
* add runtime orchestrator and serverless adapters ([65f4750](https://github.com/newstack-cloud/celerity-python-sdk/commit/65f4750c8a3f55d72d6f111dc5ab6449fdca681b))
* add sql database resource implementation ([b19a225](https://github.com/newstack-cloud/celerity-python-sdk/commit/b19a22547818e8297d2675bd9070fec902ebfd90))
* add telemetry foundations ([200b509](https://github.com/newstack-cloud/celerity-python-sdk/commit/200b509fc20552cd08a6d8325a2837b0a2484345))
* add test utils for building celerity apps ([d4cd647](https://github.com/newstack-cloud/celerity-python-sdk/commit/d4cd647cd585a1ee9f25da7d94f6e744b4261952))
* add topic resource implementation ([e305526](https://github.com/newstack-cloud/celerity-python-sdk/commit/e3055268847ebf5a0ac40b5d26d6e051968c0f30))
* complete telemetry implementation ([25863e8](https://github.com/newstack-cloud/celerity-python-sdk/commit/25863e8b2ce68a31592ee60c02517a75b29a8a30))
* integrate module-level layers ([d0bd44f](https://github.com/newstack-cloud/celerity-python-sdk/commit/d0bd44f79cc60d89feb298d80d43c8cf4ee099cf))


### Bug Fixes

* add fixes for ci integration tests and resource backend resolution ([9ee2eb6](https://github.com/newstack-cloud/celerity-python-sdk/commit/9ee2eb654b6856c724a922276da5a29d64a45e1a))
* add security fix for sql db factory to be secure when ssl is enabled ([939b9a0](https://github.com/newstack-cloud/celerity-python-sdk/commit/939b9a033ae50214928e48c961146d2538c969e5))
* add type fixes for telemetry ([75ca6c1](https://github.com/newstack-cloud/celerity-python-sdk/commit/75ca6c1c8d52ed86cfed1f915aafd1a9fc077d32))
* correct bucket resource to resolve bucket name from mappings ([548f3c8](https://github.com/newstack-cloud/celerity-python-sdk/commit/548f3c83a3e67cfb0dd7a4542f4a7d19efacc847))
* correct cache to take key prefix mappings in client constructor ([86b02b9](https://github.com/newstack-cloud/celerity-python-sdk/commit/86b02b9604f985fb7c9d89c6c801b589f2d5a813))
* correct data store resource to resolve table name from mappings ([85d8bef](https://github.com/newstack-cloud/celerity-python-sdk/commit/85d8bef3421da434cea7355782f55f161cf74996))
* correct missing type errors for sqlalchemy ([cfa6634](https://github.com/newstack-cloud/celerity-python-sdk/commit/cfa6634be9cdb778062fc916160b1b6880910460))
* correct resource injection to use annotated types ([ee60fa6](https://github.com/newstack-cloud/celerity-python-sdk/commit/ee60fa6366e61dea7d5a29447c69bb95681aa9b1))
