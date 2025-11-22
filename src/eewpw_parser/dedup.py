# -*- coding: utf-8 -*-
import json
from typing import List, Set
from pydantic import BaseModel

from eewpw_parser.schemas import Detection, Annotation


def canonical_json(obj: BaseModel) -> str:
    """
    Produce a deterministic JSON string for a Pydantic model instance.
    """
    return json.dumps(
        obj.dict(),
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )


def deduplicate_detections(detections: List[Detection]) -> List[Detection]:
    seen: Set[str] = set()
    unique: List[Detection] = []
    for d in detections:
        key = canonical_json(d)
        if key in seen:
            continue
        seen.add(key)
        unique.append(d)
    return unique


def deduplicate_annotations(annotations: List[Annotation]) -> List[Annotation]:
    seen: Set[str] = set()
    unique: List[Annotation] = []
    for a in annotations:
        key = canonical_json(a)
        if key in seen:
            continue
        seen.add(key)
        unique.append(a)
    return unique
