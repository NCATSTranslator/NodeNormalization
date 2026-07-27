# Using NodeNorm with AI Agents and LLMs

NodeNorm can be used directly from an AI coding agent (such as Claude Code) or any LLM-based tool
that can make HTTP requests. A skill file provides the instructions an agent needs to call NodeNorm
correctly — not just which endpoints exist, but which conflation settings to choose, how to batch a
large set of identifiers, and which behaviours will otherwise catch it out.

## The skill file

The skill lives at [`skills/nodenorm/SKILL.md`](../skills/nodenorm/SKILL.md). It covers:

- When NodeNorm is the right service and when you want [NameRes](https://github.com/NCATSTranslator/NameResolution) instead
- Normalizing a single identifier with `GET /get_normalized_nodes`, and how to read the response
- Normalizing many identifiers at once with `POST /get_normalized_nodes`
- Choosing GeneProtein and DrugChemical conflation deliberately, including the fact that the
  defaults differ between GET and POST ([#398](https://github.com/NCATSTranslator/NodeNormalization/issues/398))
- Recipes for the common tasks: connecting an identifier to another database, normalizing a column
  in a file, and testing whether two identifiers mean the same thing
- The non-obvious behaviours: unnormalizable CURIEs come back as `null` rather than being omitted,
  the preferred label is not necessarily the preferred identifier's label, and the data is a fixed
  Babel snapshot rather than a live database

Apart from the YAML frontmatter, which is packaging metadata for Claude Code, the file is plain
Markdown with no agent-specific syntax.

## Installing it in Claude Code

Claude Code discovers skills as `<skill-name>/SKILL.md` inside a `skills` directory — copy the whole
`skills/nodenorm/` directory, not just the file.

**For one project:**

```bash
mkdir -p .claude/skills
git clone --depth 1 --filter=blob:none --sparse \
  https://github.com/NCATSTranslator/NodeNormalization.git /tmp/nn \
  && (cd /tmp/nn && git sparse-checkout set skills/nodenorm) \
  && cp -r /tmp/nn/skills/nodenorm .claude/skills/
```

**For every session**, use `~/.claude/skills` instead of `.claude/skills`.

Either way you can also just create the directory and save the file into it by hand:

```bash
mkdir -p ~/.claude/skills/nodenorm
curl -o ~/.claude/skills/nodenorm/SKILL.md \
  https://raw.githubusercontent.com/NCATSTranslator/NodeNormalization/main/skills/nodenorm/SKILL.md
```

Claude Code will then offer it as `/nodenorm`, and will also load it automatically when a task
involves normalizing identifiers.

## Fetching it from a running instance

Every NodeNorm instance serves the same instructions at `/llms.txt`, with the frontmatter stripped:

```bash
curl https://nodenormalization-sri.renci.org/llms.txt
```

This is the quickest route for an agent that has been given a NodeNorm URL and nothing else, and it
guarantees the instructions match the deployed version. It is also linked from the API description,
so an agent reading `/openapi.json` will find it.

## Using it with other agents

For any agent that accepts a system prompt or context document, paste the contents of
[`skills/nodenorm/SKILL.md`](../skills/nodenorm/SKILL.md) (or the output of `/llms.txt`) into its
instructions. The skill is self-contained and depends on no external tooling.

## Example tasks

Once installed, these should work without further explanation:

```
Normalize MESH:D014867 and tell me what it is.

I have a UniProtKB accession, UniProtKB:P11532. What is the corresponding NCBI Gene ID?

Here's a CSV with an "identifier" column of mixed CURIEs. Normalize them all,
add a "preferred_id" column, and tell me which ones failed.

Are CHEBI:15377 and PUBCHEM.COMPOUND:962 the same thing?
```

## Keeping it accurate

The skill states specific facts about the API — parameter defaults, response shape, and worked
examples with real identifier counts. If you change any of those, update
[`skills/nodenorm/SKILL.md`](../skills/nodenorm/SKILL.md) in the same PR. `/llms.txt` is served
directly from that file, so there is only one copy to maintain.
