# NodeNorm Translator "Hammerhead" November 2024 Release
- Babel: [2024oct24](https://stars.renci.org/var/babel_outputs/2024oct24/)
  ([Babel Translator November 2024 Release](https://github.com/TranslatorSRI/Babel/blob/master/releases/TranslatorHammerheadNovember2024.md))
- NodeNorm: [v2.3.18](https://github.com/TranslatorSRI/NodeNormalization/releases/tag/v2.3.18)

Next release: _None as yet_
Previous release: [Translator "Guppy" August 2024](./TranslatorGuppyAugust2024.md)

## New features
* Add a status endpoint with database name, key count and memory usage for each database ([#268](https://github.com/TranslatorSRI/NodeNormalization/pull/268)).
* Upgraded OTEL implementation to use gRPC by @EvanDietzMorris ([#298](https://github.com/TranslatorSRI/NodeNormalization/pull/298)).

## Updates
* Updated NodeNorm preferred label to match Babel's ([#300](https://github.com/TranslatorSRI/NodeNormalization/pull/300)).

## Documentation
* Added Guppy release notes ([#294](https://github.com/TranslatorSRI/NodeNormalization/pull/294)).

## Babel updates (from [Babel Translator "Hammerhead" November 2024 Release](https://github.com/TranslatorSRI/Babel/blob/master/releases/TranslatorHammerheadNovember2024.md))
- [New features] Added taxon information to proteins ([#349](https://github.com/TranslatorSRI/Babel/pull/349))
- [Updates] Upgraded RxNorm to 09032024.
- [Updates] Changed NCBIGene download from FTP to HTTP.
- [Updates] Increased DRUG_CHEMICAL_SMALLER_MAX_LABEL_LENGTH (introduced in [#330](https://github.com/TranslatorSRI/Babel/pull/330)) from 30 to 40.

## Releases since [Translator "Guppy" August 2024](./TranslatorGuppyAugust2024.md)
* [2.3.17](https://github.com/TranslatorSRI/NodeNormalization/releases/tag/v2.3.17)
  * Add a status endpoint with database name, key count and memory usage for each database ([#268](https://github.com/TranslatorSRI/NodeNormalization/pull/268)).
  * Added Guppy release notes ([#294](https://github.com/TranslatorSRI/NodeNormalization/pull/294)).
* [2.3.18](https://github.com/TranslatorSRI/NodeNormalization/releases/tag/v2.3.18)
  * Upgraded OTEL implementation to use gRPC by @EvanDietzMorris ([#298](https://github.com/TranslatorSRI/NodeNormalization/pull/298)).
  * Updated NodeNorm preferred label to match Babel's ([#300](https://github.com/TranslatorSRI/NodeNormalization/pull/300)).
