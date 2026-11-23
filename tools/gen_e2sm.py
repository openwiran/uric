#!/usr/bin/env python3
"""Regenerate the pycrate E2SM modules in ``uric/codec/_gen`` from ``uric/asn1``.

pycrate's runtime shares ASN.1 objects between all the places where a type is
referenced. With *recursive* types (E2SM-RC RANParameter-ValueType,
RANParameter-Definition and RANParameter-Testing) the nested levels therefore
overwrite each other's values while encoding/decoding. To avoid that, the
recursive type groups are unrolled up to ``DEPTH`` levels into distinct type
names (``X-L1``, ``X-L2``...). APER encodings do not carry type names, so the
result is bit-exact with the original specification.

Usage: python tools/gen_e2sm.py
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ASN_DIR = os.path.join(ROOT, "uric", "asn1")
OUT_DIR = os.path.join(ROOT, "uric", "codec", "_gen")
DEPTH = 12

# (entry type, other members of the recursion cycle)
RC_GROUPS = [
    ("RANParameter-ValueType", [
        "RANParameter-ValueType-Choice-Structure", "RANParameter-ValueType-Choice-List",
        "RANParameter-STRUCTURE", "RANParameter-STRUCTURE-Item", "RANParameter-LIST"]),
    ("RANParameter-Definition", [
        "RANParameter-Definition-Choice", "RANParameter-Definition-Choice-LIST",
        "RANParameter-Definition-Choice-LIST-Item", "RANParameter-Definition-Choice-STRUCTURE",
        "RANParameter-Definition-Choice-STRUCTURE-Item"]),
    ("RANParameter-Testing-Item", [
        "RANParameter-Testing-Item-Choice-List", "RANParameter-Testing-Item-Choice-Structure",
        "RANParameter-Testing-LIST", "RANParameter-Testing-STRUCTURE"]),
]

_ASSIGN = re.compile(r"^([A-Za-z][\w-]*)\s*::=", re.M)


def _ref(name: str) -> re.Pattern:
    return re.compile(r"(?<![\w-])" + re.escape(name) + r"(?![\w-])")


def unroll(text: str, module: str, groups, depth: int) -> str:
    start = text.index(module)
    end = text.index("\nEND", start)
    body = text[start:end]
    matches = list(_ASSIGN.finditer(body))
    defs = {}
    for i, m in enumerate(matches):
        stop = matches[i + 1].start() if i + 1 < len(matches) else len(body)
        defs[m.group(1)] = (m.start(), stop)

    def lvl(name: str, k: int) -> str:
        return name if k == 0 else f"{name}-L{k}"

    replacements = {}  # (start, stop) -> new text for level 0 definitions
    extra = []
    for entry, members in groups:
        names = [entry] + members
        for k in range(depth + 1):
            nxt = min(k + 1, depth)
            for n in names:
                s, e = defs[n]
                d = body[s:e]
                # split "Name ::= rest" so that the defined name is not rewritten
                head, rest = d.split("::=", 1)
                for other in sorted(names, key=len, reverse=True):
                    target = lvl(other, nxt) if other == entry else lvl(other, k)
                    rest = _ref(other).sub(target, rest)
                new = f"{lvl(n, k)} ::={rest}"
                if k == 0:
                    replacements[(s, e)] = new
                else:
                    extra.append(new.rstrip() + "\n\n")
    out = []
    pos = 0
    for (s, e), new in sorted(replacements.items()):
        out.append(body[pos:s])
        out.append(new)
        pos = e
    out.append(body[pos:])
    new_body = "".join(out).rstrip() + "\n\n-- unrolled recursive types (generated)\n\n" + "".join(extra)
    return text[:start] + new_body + text[end:]


def compile_asn(src: str, out: str) -> None:
    compiler = os.path.join(os.path.dirname(sys.executable), "pycrate_asn1compile.py")
    cmd = [sys.executable, compiler, "-i", src, "-o", out]
    subprocess.run(cmd, check=True)


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        kpm = os.path.join(ASN_DIR, "e2sm_kpm_v03.00.asn1")
        compile_asn(kpm, os.path.join(OUT_DIR, "e2sm_kpm_asn"))

        with open(os.path.join(ASN_DIR, "e2sm_rc_v01.03.asn1")) as f:
            rc = f.read()
        rc = unroll(rc, "E2SM-RC-IEs {", RC_GROUPS, DEPTH)
        rc_src = os.path.join(tmp, "e2sm_rc_unrolled.asn1")
        with open(rc_src, "w") as f:
            f.write(rc)
        compile_asn(rc_src, os.path.join(OUT_DIR, "e2sm_rc_asn"))
    print("generated modules in", OUT_DIR)


if __name__ == "__main__":
    main()
