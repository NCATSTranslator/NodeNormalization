# set_id.py
# Code related to generating IDs for sets (as in https://github.com/TranslatorSRI/NodeNormalization/issues/256).
import base64
import gzip
import hashlib
import logging
import uuid

import zlib

from .model import SetIDResponse
from .normalizer import get_normalized_nodes

# UUID namespace for SetIDs
uuid_namespace_setid = uuid.UUID('14ef168c-14cb-4979-8442-da6aaca55572')


async def generate_setid(app, curies, conflations) -> SetIDResponse:
    """
    Generate a SetID for a set of curies.

    :param app: The NodeNorm app (used to access the databases).
    :param curies: A list of curies to generate a set ID for.
    :param conflations: A list of conflations to apply. Must be one or both of 'GeneProtein' and 'DrugChemical'.
    :return: A SetIDResponse with the Set ID.
    """

    # Step 0. Prepare the SetIDResponse by filling it with the arguments.
    response = SetIDResponse(
        curies=curies,
        conflations=conflations
    )

    # Step 1. Normalize the curies given the conflation settings.
    gene_protein_conflation = "GeneProtein" in conflations
    drug_chemical_conflation = "DrugChemical" in conflations
    if not all(item in ['GeneProtein', 'DrugChemical'] for item in conflations):
        response.error = "Conflations provided to " + \
            f"generate_setid() are {conflations}, but only 'GeneProtein' and 'DrugChemical' are allowed."
        return response

    # We use get_normalized_nodes() to normalize all the CURIEs for us.
    normalization_results = await get_normalized_nodes(
        app, curies, gene_protein_conflation, drug_chemical_conflation, include_descriptions=False, include_individual_types=False, include_taxa=False
    )

    # We prepare a set of sorted, deduplicated curies.
    curies_normalized_already = set()
    normalized_curies = []
    for curie in curies:
        # CURIE must be a string.
        curie = str(curie)
        if curie in normalization_results and normalization_results[curie] is not None:
            result = normalization_results[curie]
            if 'id' in result and 'identifier' in result['id']:
                preferred_id = result['id']['identifier']
                if preferred_id in curies_normalized_already:
                    # Don't duplicate normalized IDs: that way if someone queries for ['id1', 'id2', 'id3'] where
                    # they normalize to ['nr1', 'nr2', 'nr2'], we can come up with the set ['nr1', 'nr2'], which will
                    # be a better set_id().
                    pass
                else:
                    normalized_curies.append(preferred_id)
                    curies_normalized_already.add(preferred_id)
            else:
                # We got back a normalization response, but no preferred ID. This shouldn't happen.
                logging.warning(
                    f"Normalized CURIE {curie} returned a response but not a preferred identifier: {normalization_results[curie]}"
                )
                normalized_curies.append(curie)
                curies_normalized_already.add(curie)
        else:
            # No normalized identifier.
            normalized_curies.append(curie)
            curies_normalized_already.add(curie)

    sorted_normalized_curies = sorted(normalized_curies)
    response.normalized_curies = sorted_normalized_curies

    # Do we have any normalized CURIEs? If not, return now.
    if not sorted_normalized_curies:
        return response

    normalized_string = "||".join(sorted_normalized_curies)
    response.normalized_string = normalized_string

    # There are several options we've tried here:
    # - SHA224 hash -- but this is too long.
    # response.sha224hash = hashlib.sha224(normalized_string.encode('utf-8')).hexdigest()

    # - base64+zip, so it would be reversible, which might be something we want at some point
    #   (https://github.com/TranslatorSRI/NodeNormalization/issues/256#issuecomment-2197465751),
    #   but that is also too long.
    # response.base64 = base64.b64encode(normalized_string.encode('utf-8')).decode('utf-8')
    # compressed_normalized_string = zlib.compress(normalized_string.encode('utf-8'))
    # response.base64zlib = base64.b64encode(compressed_normalized_string).decode('utf-8')

    # - UUID v5 identifiers with a custom namespace.
    response.setid = 'uuid:' + str(uuid.uuid5(uuid_namespace_setid, normalized_string))

    return response
