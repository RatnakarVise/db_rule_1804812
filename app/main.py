from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional, Tuple
import re
import json

app = FastAPI(
    title="ABAP MB Transaction Remediator (SAP Note 1804812)"
)

OBSOLETE_MB_TXNS = [
    "MB01", "MB02", "MB03", "MB04", "MB05", "ΜΒΘΑ", "MB11",
    "MB1A", "MB18", "MBC", "MB31", "MBNL", "MBRL", "MBSF",
    "MBSL", "MBST", "MBSU"
]

MB_TXN_RE = re.compile(
    rf"""
    (?P<full>
        (?P<stmt>CALL\s+TRANSACTION|SUBMIT)
        \s+
        ['"]?(?P<txn>{'|'.join(OBSOLETE_MB_TXNS)})['"]?
        \s*\.?
    )
    """,
    re.IGNORECASE | re.VERBOSE
)

class Unit(BaseModel):
    pgm_name: str
    inc_name: str
    type: str
    name: Optional[str] = None
    class_implementation: Optional[str] = None
    start_line: Optional[int] = None
    end_line: Optional[int] = None
    code: Optional[str] = ""

def suggest_replacement(stmt: str) -> str:
    stmt_up = stmt.upper()
    if stmt_up.startswith("SUBMIT"):
        return "SUBMIT MIGO."
    else:
        return "CALL TRANSACTION 'MIGO'."

def find_mb_txn_usage(txt: str):
    matches = []
    for m in MB_TXN_RE.finditer(txt):
        stmt = m.group("stmt")
        txn = m.group("txn")
        full_stmt = m.group("full")
        suggested = suggest_replacement(stmt)
        matches.append({
            "full": full_stmt,
            "stmt": stmt,
            "txn": txn,
            "suggested_statement": suggested,
            "span": m.span("full")
        })
    return matches

# def apply_span_replacements(src: str, repls: List[Tuple[Tuple[int,int], str]]) -> str:
#     out = src
#     for (s, e), r in sorted(repls, key=lambda x: x[0][0], reverse=True):
#         out = out[:s] + r + out[e:]
#     return out

@app.post("/remediate-mb-txns")
def remediate_mb_txns(units: List[Unit]):
    results = []
    for u in units:
        src = u.code or ""
        matches = find_mb_txn_usage(src)
        replacements = []
        metadata = []

        for m in matches:
            replacements.append((m["span"], m["suggested_statement"]))
            metadata.append({
                "table": "None",
                "target_type": "None",
                "target_name": "None",
                "used_fields": [],
                "ambiguous": False,
                "obsolete_mb_txn": m["txn"],
                "obsolete_txn": m["txn"],
                "start_char_in_unit": m["span"][0],
                "end_char_in_unit": m["span"][1],
                "suggested_statement":m["suggested_statement"],
                "suggested_fields": None,
                "note": "Replace obsolete MB transaction with MIGO per SAP Note 1804812."
            })

        # modified = apply_span_replacements(src, replacements)
        obj = json.loads(u.model_dump_json())
        # obj["code"] = modified
        obj["mb_txn_usage"] = metadata
        results.append(obj)

    return results
