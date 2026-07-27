# NodeNorm Translator December 2023 Release

- Babel: [2023nov5](https://stars.renci.org/var/babel_outputs/2023nov5/)
- NodeNorm: [v2.3.5](https://github.com/NCATSTranslator/NodeNormalization/releases/tag/v2.3.5)

Next release: [May 2024](TranslatorMay2024.md)

## New features
* Updated TRAPI message normalization code to TRAPI 1.4
* Chemical Conflation
  * Conflates chemicals that essentially refer to the same drug as determined through RXCUIs.
  * Turned off by default; can be turned on with the `drug_chemical_conflate` flag in
    [GET `/get_normalized_nodes`](https://nodenorm.test.transltr.io/docs#/default/get_normalized_node_handler_get_normalized_nodes_get)
    and
    [POST `/get_normalized_nodes`](https://nodenorm.test.transltr.io/docs#/default/get_normalized_node_handler_get_normalized_nodes_post).
* `description` flag can be used to include descriptions in node normalization results in
  [GET `/get_normalized_nodes`](https://nodenorm.test.transltr.io/docs#/default/get_normalized_node_handler_get_normalized_nodes_get)
  and
  [POST `/get_normalized_nodes`](https://nodenorm.test.transltr.io/docs#/default/get_normalized_node_handler_get_normalized_nodes_post).
* [`/get_semantic_types`](https://nodenorm.test.transltr.io/docs#/default/get_semantic_types_handler_get_semantic_types_get)
  is now functional again.
* Minor fixes to the Terms of Service and service description.
* Added OTEL/Jaegar telemetry support.

## Releases since [Translator October 2023 release](TranslatorOctober2023.md)

* [NodeNorm v2.2.1](https://github.com/NCATSTranslator/NodeNormalization/releases/tag/v2.2.1)
  * Node descriptions by @YaphetKG in #212
  * Trapi 14 by @cbizon in #208
* [NodeNorm v2.3.0](https://github.com/NCATSTranslator/NodeNormalization/releases/tag/v2.3.0)
  * Node descriptions by @YaphetKG in #216
  * initial implementation chem conflation by @cbizon in #211
* [NodeNorm v2.3.1](https://github.com/NCATSTranslator/NodeNormalization/releases/tag/v2.3.1)
  * Fix Terms of Service and service description by @gaurav in #226
* [NodeNorm v2.3.2](https://github.com/NCATSTranslator/NodeNormalization/releases/tag/v2.3.2)
  * Bump httpx from 0.19.0 to 0.23.0 by @dependabot in #195
  * Bump requests from 2.28.1 to 2.31.0 by @dependabot in #194
  * Upgrade reasoner-pydantic and requirements.lock by @gaurav in #227
* [NodeNorm v2.3.3](https://github.com/NCATSTranslator/NodeNormalization/releases/tag/v2.3.3)
  * Add NodeNorm-Loader Docker by @gaurav in #228
  * Fix missing node bindings in NodeNorm /query output by @gaurav in #231
  * Uniquify semantic types returned from the database by @gaurav in #232
  * Remove hardcoded TRAPI version by @gaurav in #233
  * Remove biolink:Entity from nodes created by NodeNorm by @gaurav in #234
* [NodeNorm v2.3.4](https://github.com/NCATSTranslator/NodeNormalization/releases/tag/v2.3.4)
  * Forgot to update the version number in NodeNorm v2.3.3.
* [NodeNorm v2.3.5](https://github.com/NCATSTranslator/NodeNormalization/releases/tag/v2.3.5)
  * Workaround for missing type information by @gaurav in #223
  * Add Open Telemetry instrumentation by @gaurav in #237
