# NodeNorm Translator "Fugu" July 2024 Release
- Babel: [2024jul13](https://stars.renci.org/var/babel_outputs/2024jul13/)
  ([Babel Translator July 2024 Release](https://github.com/TranslatorSRI/Babel/blob/master/releases/TranslatorFuguJuly2024.md))
- NodeNorm: [v2.3.15](https://github.com/TranslatorSRI/NodeNormalization/releases/tag/v2.3.15)

Previous release: [Translator May 2024](./TranslatorMay2024.md)
Next release: [Translator "Guppy" August 2024](./TranslatorGuppyAugust2024.md)

## New features
* Update Biolink model by replacing bmt-lite with bmt ([#282](https://github.com/TranslatorSRI/NodeNormalization/pull/282),
  [#284](https://github.com/TranslatorSRI/NodeNormalization/pull/284))
* Added /get_setid GET endpoint need for Multi-CURIE Query (MCQ)  ([#274](https://github.com/TranslatorSRI/NodeNormalization/pull/274))

## Technical debt
* Improved database names in source code.

## Babel updates (from [Babel Translator July 2024 Release](https://github.com/TranslatorSRI/Babel/blob/master/releases/TranslatorFuguJuly2024.md))
* [Feature] Added manual disease concords, and used that to do a better job of combining opioid use disorder and alcohol use disorder.
* [Feature] Moved ensembl_datasets_to_skip into the config file.
* [Bugfix] Eliminated preferred prefix overrides in Babel; we now trust the preferred prefixes from the Biolink Model.
* [Bugfix] DrugChemical conflation generation now removes CURIEs that can't be normalized.
* [Bugfix] Replaced http://nihilism.com/ with http://example.org/ as a base IRI.
* [Bugfix] Updated mappings from Reactome types to Biolink types.
* [Update] Updated Biolink from 4.1.6 to 4.2.1.
* [Update] Updated UMLS from 2023AB to 2024AA.
* [Update] Updated RxNorm from 03042024 to 07012024.
* [Update] Updated PANTHER_Sequence_Classification from PTHR18.0_human to PTHR19.0_human.
* [Update] Updated PANTHER pathways from SequenceAssociationPathway3.6.7.txt to SequenceAssociationPathway3.6.8.txt.

## Releases since [Translator May 2024 release](TranslatorMay2024.md)
* [NodeNorm v2.3.12](https://github.com/TranslatorSRI/NodeNormalization/releases/tag/v2.3.12)
  * Rename redis_connection[0-6] to their database names by @gaurav in #271
* [NodeNorm v2.3.13](https://github.com/TranslatorSRI/NodeNormalization/releases/tag/v2.3.13): Incremented version,
  which I forgot to do in the previous release.
* [NodeNorm v2.3.14](https://github.com/TranslatorSRI/NodeNormalization/releases/tag/v2.3.14)
  * Replaced bmt-lite with bmt by @gaurav in #282
  * Add a /get_setid GET endpoint by @gaurav in #274
* [NodeNorm v2.3.15](https://github.com/TranslatorSRI/NodeNormalization/releases/tag/v2.3.15)
  * Fix BMT import and formatting issues by @gaurav in #284
