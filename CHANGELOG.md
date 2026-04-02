# Changelog

## [0.5.0](https://github.com/newstack-cloud/celerity-python-sdk/compare/v0.4.0...v0.5.0) (2026-04-02)


### Features

* add testing helpers for unit, integration and api tests ([042538c](https://github.com/newstack-cloud/celerity-python-sdk/commit/042538cfdb323bfd2a0599a4f638c0b0eedc2f96))


### Bug Fixes

* remove hardcoded fallback db connection string for test sql client ([784cf01](https://github.com/newstack-cloud/celerity-python-sdk/commit/784cf01dba1c51a933624cf397faf6c47cfda78d))

## [0.4.0](https://github.com/newstack-cloud/celerity-python-sdk/compare/v0.3.0...v0.4.0) (2026-03-23)


### Features

* add helper to capture aws credentials for celerity resource aws backends ([dfc520d](https://github.com/newstack-cloud/celerity-python-sdk/commit/dfc520dfbda26cadea7791a0a4b28bd8b4d7fcd2))
* add helper to support typed message bodies when publishing messages ([56ad215](https://github.com/newstack-cloud/celerity-python-sdk/commit/56ad215cbba5ef6e34f2808f2675183fae7bde10))
* add resolve hooks to allow for lazily resolving deps like typed config ([65a4a06](https://github.com/newstack-cloud/celerity-python-sdk/commit/65a4a06c1807c8987953acff703d60d8536493d2))
* add support for new credentials extraction for s3 and dynamo backends ([57800f2](https://github.com/newstack-cloud/celerity-python-sdk/commit/57800f2016d87041512e8755d7a96cdcbb661328))
* enhance config resource to support typed extraction ([7df0e54](https://github.com/newstack-cloud/celerity-python-sdk/commit/7df0e5467d7e41411c6963900e2d1038264215c0))
* update service container interface to support adding resolve hooks ([a16be4c](https://github.com/newstack-cloud/celerity-python-sdk/commit/a16be4c1cc697520c2d9bae1c26748f9ad8b5c1b))


### Bug Fixes

* add fix to correctly extract websocket message body ([25ebec5](https://github.com/newstack-cloud/celerity-python-sdk/commit/25ebec5d90725b22bafb284dbb98d5ad6a2c4d3a))
* add fixes for event-based handler routing ([2009a8d](https://github.com/newstack-cloud/celerity-python-sdk/commit/2009a8d70612aa4058b7d962bedd0363cf0d3410))
* add fixes for module resolution ([ba208d6](https://github.com/newstack-cloud/celerity-python-sdk/commit/ba208d61099d9f46e2e0d4dab1d8eca43d3ffdf5))
* add missing auth context for websocket message mapping ([161d3f3](https://github.com/newstack-cloud/celerity-python-sdk/commit/161d3f34b86c0673f215f9364a17277c8144b3e6))
* add typed message body support and fixes to physical resource id resolution ([7fc335f](https://github.com/newstack-cloud/celerity-python-sdk/commit/7fc335f336e1d3c172f63652d71388a0e63b05ef))
* correct method name used for guards ([41519a3](https://github.com/newstack-cloud/celerity-python-sdk/commit/41519a3e9517e9059d75319c52224f8b5094c55c))
* ensure handlers are resolved lazily for dynamic di ([7771f28](https://github.com/newstack-cloud/celerity-python-sdk/commit/7771f283187d594ae222aa7f0e707d20c23b31c5))
* improve and standardise api consumer validation messages ([da5e953](https://github.com/newstack-cloud/celerity-python-sdk/commit/da5e9532315e4a8c0f291e5e45ddd00e22a3e918))


### Dependencies

* bump celerity-runtime-sdk to 0.2.1 ([08ef748](https://github.com/newstack-cloud/celerity-python-sdk/commit/08ef748c911198c1bd60417865f8fda73393c756))

## [0.3.0](https://github.com/newstack-cloud/celerity-python-sdk/compare/v0.2.1...v0.3.0) (2026-03-21)


### Features

* add improvements to dx for param extraction and http response handling ([1dc986d](https://github.com/newstack-cloud/celerity-python-sdk/commit/1dc986d56b29998ca3e8405b641e61d40b6c904e))

## [0.2.1](https://github.com/newstack-cloud/celerity-python-sdk/compare/v0.2.0...v0.2.1) (2026-03-21)


### Bug Fixes

* add fix to make start_runtime synchronous ([0cc656f](https://github.com/newstack-cloud/celerity-python-sdk/commit/0cc656f1d892d72cd9e4d23317de3d1ee32c16d6))

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
