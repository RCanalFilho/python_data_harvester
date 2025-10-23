
from __future__ import annotations
import json, os, datetime

class RunReport:
    def __init__(self):
        self.steps = []
        self.errors = []
        self.artifacts = []

    def add_step(self, name, meta=None):
        self.steps.append({'name': name, 'meta': meta or {}, 'ts': datetime.datetime.now().isoformat()})

    def add_error(self, where, err):
        self.errors.append({'where': where, 'err': str(err)})

    def add_artifact(self, path, kind):
        self.artifacts.append({'path': path, 'kind': kind})

    def to_json(self, out_path):
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump({'steps': self.steps, 'errors': self.errors, 'artifacts': self.artifacts}, f, indent=2)

    def summary_text(self):
        lines = []
        lines.append("=== gee-pipeline Run Summary ===")
        lines.append(f"Steps: {len(self.steps)} | Artifacts: {len(self.artifacts)} | Errors: {len(self.errors)}\n")
        for s in self.steps:
            lines.append(f"• {s['ts']} — {s['name']} ({len(s['meta'])} meta)")
        if self.artifacts:
            lines.append("\nArtifacts:")
            for a in self.artifacts:
                lines.append(f"  - [{a['kind']}] {a['path']}")
        if self.errors:
            lines.append("\nErrors:")
            for e in self.errors:
                lines.append(f"  - {e['where']}: {e['err']}")
        return "\n".join(lines)
