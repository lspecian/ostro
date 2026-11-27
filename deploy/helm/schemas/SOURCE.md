# KEDA CRD JSON schemas (kubeconform)

Used by `scripts/helm-check.sh` (`kubeconform -schema-location
'deploy/helm/schemas/{{ .ResourceKind }}_{{ .ResourceAPIVersion }}.json'`) to validate the
chart's and `deploy/helm/ostro/examples/keda/`'s KEDA objects offline. Phase 14, 14-13.

- Source: `https://github.com/kedacore/keda/releases/download/v2.21.1/keda-2.21.1-crds.yaml`
- sha256: `a4da97cdcb2ed132de343221aea13e505f0e5151e063840b0eca775cd4e17cfb`
- Downloaded on 2026-11-27 into its own empty directory under `target/`, checksum verified
  before conversion.
- Conversion (strict: `additionalProperties: false` on every object schema with
  `properties` and without `x-kubernetes-preserve-unknown-fields`; int-or-string and
  nullable mapped; `apiVersion`/`kind` pinned):

  ```
  python3 -I scripts/crd2jsonschema.py <dir>/keda-2.21.1-crds.yaml deploy/helm/schemas \
    ScaledObject TriggerAuthentication ScaledJob ClusterTriggerAuthentication
  ```

Files (kubeconform naming `<kind lowercase>_<version>.json`):

| File | CRD |
|------|-----|
| `scaledobject_v1alpha1.json` | `scaledobjects.keda.sh` v1alpha1 |
| `triggerauthentication_v1alpha1.json` | `triggerauthentications.keda.sh` v1alpha1 |
| `scaledjob_v1alpha1.json` | `scaledjobs.keda.sh` v1alpha1 |
| `clustertriggerauthentication_v1alpha1.json` | `clustertriggerauthentications.keda.sh` v1alpha1 |

To move to another KEDA release: download the new CRD file into a fresh directory, verify
its published checksum, rerun the command above, update this file and rerun
`scripts/helm-check.sh`.
