"""
00_build_vocab.py
────────────────────────────────────────────────────────
Parse the local data/cleaned/ordo.owl (v4.5) and
write English labels + synonyms to data/cleaned/ordo_terms.tsv
"""

import pathlib, sys
from rdflib import Graph, RDFS, Literal, Namespace, URIRef

BASE = pathlib.Path(__file__).resolve().parents[1]
OWL  = BASE / "data" / "cleaned" / "ordo.owl"
TSV  = BASE / "data" / "cleaned" / "ordo_terms.tsv"

OBO  = Namespace("http://www.geneontology.org/formats/oboInOwl#")
HAS_SYN = URIRef(OBO + "hasExactSynonym")

if not OWL.exists():
    sys.exit(f"❌  {OWL} not found – place the unzipped ordo.owl there first.")

print("🔍  Parsing ORDO …")
g = Graph()
g.parse(OWL, format="xml")

terms = set()

# English rdfs:label
for _, _, lbl in g.triples((None, RDFS.label, None)):
    if isinstance(lbl, Literal) and (lbl.language in (None, "en")):
        terms.add(lbl.value.strip())

# Exact synonyms
for _, _, syn in g.triples((None, HAS_SYN, None)):
    if isinstance(syn, Literal):
        terms.add(syn.value.strip())

TSV.write_text("\n".join(sorted(terms)), encoding="utf-8")
print(f"✅  Saved {len(terms):,} terms → {TSV}")
