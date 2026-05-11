# [1.3.0](https://github.com/G2BC/BEM-api/compare/v1.2.4...v1.3.0) (2026-05-11)


### Features

* **species_repository:** remove photo priority logic and streamline species count query for improved readability ([d2e7592](https://github.com/G2BC/BEM-api/commit/d2e7592cfdd1c20d994085beb4ad93a1a6866c49))

## [1.2.4](https://github.com/G2BC/BEM-api/compare/v1.2.3...v1.2.4) (2026-05-11)


### Bug Fixes

* update cache prefixes for observations and NCBI to include 'bem:' namespace ([787454b](https://github.com/G2BC/BEM-api/commit/787454b9605f460281d9ff50a2be4e07ce675d1d))

## [1.2.3](https://github.com/G2BC/BEM-api/compare/v1.2.2...v1.2.3) (2026-05-11)


### Bug Fixes

* streamline dotenv loading in application initialization ([aaf28d9](https://github.com/G2BC/BEM-api/commit/aaf28d996f3a669375e28fa269b3e70ff538ddbf))

## [1.2.2](https://github.com/G2BC/BEM-api/compare/v1.2.1...v1.2.2) (2026-05-11)


### Bug Fixes

* simplify dotenv loading by removing specific path ([13d253e](https://github.com/G2BC/BEM-api/commit/13d253e84d4ba94f5b371cacb776a74b1aa9b6cb))

## [1.2.1](https://github.com/G2BC/BEM-api/compare/v1.2.0...v1.2.1) (2026-05-11)


### Bug Fixes

* update dotenv loading to use .env.vault path ([3426606](https://github.com/G2BC/BEM-api/commit/3426606a49daa2b924806080f0b90d1b2e443ac4))

# [1.2.0](https://github.com/G2BC/BEM-api/compare/v1.1.4...v1.2.0) (2026-05-11)


### Features

* update production environment variables in .env.vault ([c01783c](https://github.com/G2BC/BEM-api/commit/c01783cede9ffd5fe38fdcdda9d546b46388744e))

## [1.1.4](https://github.com/G2BC/BEM-api/compare/v1.1.3...v1.1.4) (2026-05-11)


### Bug Fixes

* update README to include usage of dotend-vault ([a5c4ce4](https://github.com/G2BC/BEM-api/commit/a5c4ce493d186f793de515e75179a0683995c898))

## [1.1.3](https://github.com/G2BC/BEM-api/compare/v1.1.2...v1.1.3) (2026-05-11)


### Bug Fixes

* remove force recreate option from Docker compose command during deployment ([e712886](https://github.com/G2BC/BEM-api/commit/e712886e4834b036664ad9a8899862229d1de04c))

## [1.1.2](https://github.com/G2BC/BEM-api/compare/v1.1.1...v1.1.2) (2026-05-11)


### Bug Fixes

* update Docker compose command to force recreate containers during deployment ([41f62eb](https://github.com/G2BC/BEM-api/commit/41f62ebe5e8a4bec5af08f27962058f6c1629449))

## [1.1.1](https://github.com/G2BC/BEM-api/compare/v1.1.0...v1.1.1) (2026-05-11)


### Bug Fixes

* add REDIS_PASSWORD to .env.example for environment configuration ([b9d7490](https://github.com/G2BC/BEM-api/commit/b9d749099c4daa25fe9601c87f78375531036283))

# [1.1.0](https://github.com/G2BC/BEM-api/compare/v1.0.0...v1.1.0) (2026-05-11)


### Features

* add new API endpoints for documentation and OpenAPI specification ([08dcfe4](https://github.com/G2BC/BEM-api/commit/08dcfe4fc0b38ac50e47faad589891a3a0ef0e9e))

# 1.0.0 (2026-05-10)


### Bug Fixes

* set default BOLD_GEO_QUERY to Brazil for improved data synchronization ([8509ae5](https://github.com/G2BC/BEM-api/commit/8509ae5d8f8799299e9e62e6c2390a87fc4a12b9))
* update default bucket name in SnapshotDownload route from 'lumm-db' to 'bem-db' ([192e7f8](https://github.com/G2BC/BEM-api/commit/192e7f889cfcdde09de559338ab4faa4ceb5b798))
* update place_id in fetch_page function for accurate location filtering in iNaturalist synchronization ([787a695](https://github.com/G2BC/BEM-api/commit/787a695573ecde63842318c7db7c1f665d5f114d))


### Features

* add bem filter to species search functionality ([bd9398a](https://github.com/G2BC/BEM-api/commit/bd9398a16c7e6aa4e69a94b68ab882925023ea79))
* add bem_select endpoint to retrieve distinct species BEM values ([339a28f](https://github.com/G2BC/BEM-api/commit/339a28fab31cf4279d392e23d7bf87f3f14c8f44))
* add bem, brazilian_type, and brazilian_type_synonym fields to species schemas ([16189d0](https://github.com/G2BC/BEM-api/commit/16189d04c885d02e7dd4db9579b48e743fa6ce34))
* add Brazil bounding box parameters to fetch_page function for improved data filtering in iNaturalist synchronization ([a386efa](https://github.com/G2BC/BEM-api/commit/a386efab90ac60cdcf35098823758da51b3067fd))
* add common name field to Species model ([a6d683c](https://github.com/G2BC/BEM-api/commit/a6d683c2ec7f6ef77c6e328ef3d4090ab09e49c2))
* add distribution occurrence statistics endpoint and related service method for enhanced species data analysis ([20f0b5f](https://github.com/G2BC/BEM-api/commit/20f0b5fc06a00570307ffe2336a2feb8f8702d42))
* add endpoint for species statistics and include common names in detail schemas ([454dbe0](https://github.com/G2BC/BEM-api/commit/454dbe00ba203f7e4042f72a3563d319436a4427))
* add new fields to Species model for additional classification and identification ([2b4cfc4](https://github.com/G2BC/BEM-api/commit/2b4cfc4c650349b3aa826593fb0d155ddb2c4958))
* add observations count to species query and schema for enhanced data retrieval ([a4bd887](https://github.com/G2BC/BEM-api/commit/a4bd887ffe483e1bd17d80781bfc6e9818866e74))
* add place_id parameter to fetch page function for improved location filtering in iNaturalist synchronization ([03b4923](https://github.com/G2BC/BEM-api/commit/03b4923c8b5ceea2926c1540fe44d875a9654a24))
* add Postman collection for BEM API with endpoints for system health, authentication, and user management ([5cf2e00](https://github.com/G2BC/BEM-api/commit/5cf2e00856ffd7ccb86d7f6cb0c224bcc0f402ce))
* add unique constraints for uid and common_name fields in Species model ([3587530](https://github.com/G2BC/BEM-api/commit/358753059f8d8392c35c8546b95d4935ffce3844))
* add unique identifier to Species model ([b499574](https://github.com/G2BC/BEM-api/commit/b499574ff491e5ffb81dc17de73d8e1e1e5ec1a9))
* enhance import_inaturalist_photos_release_1 script with additional photo metadata and improve project path handling ([b35c886](https://github.com/G2BC/BEM-api/commit/b35c886efffac33b99a9bb0447392163ea2f5de9))
* enhance IUCN Red List synchronization with BEM support and improve error handling ([e8b0dda](https://github.com/G2BC/BEM-api/commit/e8b0ddaa7c7faf58edc310a1320d9751113d7cd6))
* enhance species BEM query to group results and prioritize BEM entries ([cabfa1a](https://github.com/G2BC/BEM-api/commit/cabfa1ab417e9da3f0a6278559c34ff8b781ffda))
* expand Taxon model and schema with additional classification fields ([b65804a](https://github.com/G2BC/BEM-api/commit/b65804ae9cc068174b876a06f0b279a75e31e931))
* implement classification parsing and enhance MycoBank synchronization logic ([3e6eaaf](https://github.com/G2BC/BEM-api/commit/3e6eaaffd2123684238960909d220c7de126b91a))
* update MycoBank synchronization workflow and enhance taxonomy parsing functions ([4c54348](https://github.com/G2BC/BEM-api/commit/4c543486281a965dd286c8be42be5ab844b2095f))

# Changelog
