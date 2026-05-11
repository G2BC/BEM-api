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
