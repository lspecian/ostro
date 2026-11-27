#!/usr/bin/env python3
"""Convert CustomResourceDefinitions into kubeconform JSON schemas (Phase 14, 14-13).

usage: python3 -I scripts/crd2jsonschema.py <crds.yaml> <outdir> [kind ...]

For every CRD version in <crds.yaml> (a multi-document YAML file), writes
<outdir>/<kind_lowercase>_<version>.json from spec.versions[].schema.openAPIV3Schema.
That is kubeconform's `{{ .ResourceKind }}_{{ .ResourceAPIVersion }}` naming.
With kind arguments, only those kinds (case-insensitive) are written.

Strict semantics (as openapi2jsonschema --strict):
- every object schema that has `properties` and no `x-kubernetes-preserve-unknown-fields`
  gets `additionalProperties: false` (unknown fields are errors);
- `x-kubernetes-int-or-string: true` becomes `oneOf: [string, integer]`;
- `nullable: true` adds "null" to the type;
- the root gets `apiVersion`, `kind` (pinned to the CRD's group/version and kind) and
  `metadata` properties when the CRD schema lacks them.

Needs PyYAML. Run it with `python3 -I` on a downloaded, checksum-verified file in its own
directory (see deploy/helm/schemas/SOURCE.md). The output is deterministic (sorted keys).
"""

import json
import os
import sys

import yaml


def strictify(node):
    """Rewrite one schema node in place, walking only schema-valued keywords."""
    if not isinstance(node, dict):
        return
    for child in (node.get("properties") or {}).values():
        strictify(child)
    for key in ("items", "additionalProperties", "not"):
        if isinstance(node.get(key), dict):
            strictify(node[key])
    for key in ("allOf", "anyOf", "oneOf"):
        for child in node.get(key) or []:
            strictify(child)
    if node.get("x-kubernetes-int-or-string"):
        node.pop("type", None)
        node["oneOf"] = [{"type": "string"}, {"type": "integer"}]
    if node.pop("nullable", False):
        t = node.get("type")
        if isinstance(t, str):
            node["type"] = [t, "null"]
        elif isinstance(t, list) and "null" not in t:
            node["type"] = t + ["null"]
    if (
        "properties" in node
        and not node.get("x-kubernetes-preserve-unknown-fields")
        and "additionalProperties" not in node
    ):
        node["additionalProperties"] = False


def convert(crd):
    """Yield (filename, schema) for every served version of one CRD."""
    spec = crd.get("spec", {})
    group = spec["group"]
    kind = spec["names"]["kind"]
    for version in spec.get("versions", []):
        schema = (version.get("schema") or {}).get("openAPIV3Schema")
        if schema is None:
            continue
        schema = json.loads(json.dumps(schema))
        props = schema.setdefault("properties", {})
        props.setdefault("apiVersion", {"type": "string"})
        props.setdefault("kind", {"type": "string"})
        props.setdefault("metadata", {"type": "object"})
        props["apiVersion"] = dict(props["apiVersion"], enum=[f"{group}/{version['name']}"])
        props["kind"] = dict(props["kind"], enum=[kind])
        strictify(schema)
        schema["$schema"] = "http://json-schema.org/draft-07/schema#"
        yield f"{kind.lower()}_{version['name']}.json", schema


def main(argv):
    if len(argv) < 3:
        sys.stderr.write(__doc__)
        return 2
    src, outdir = argv[1], argv[2]
    wanted = {k.lower() for k in argv[3:]}
    os.makedirs(outdir, exist_ok=True)
    written = 0
    with open(src, encoding="utf-8") as f:
        for doc in yaml.safe_load_all(f):
            if not doc or doc.get("kind") != "CustomResourceDefinition":
                continue
            if wanted and doc["spec"]["names"]["kind"].lower() not in wanted:
                continue
            for name, schema in convert(doc):
                path = os.path.join(outdir, name)
                with open(path, "w", encoding="utf-8") as out:
                    json.dump(schema, out, indent=1, sort_keys=True)
                    out.write("\n")
                print(path)
                written += 1
    if written == 0:
        sys.stderr.write("crd2jsonschema: no matching CRD versions found\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
