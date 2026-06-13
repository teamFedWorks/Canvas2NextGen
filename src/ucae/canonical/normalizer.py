import copy
import hashlib
import json
from dataclasses import asdict
from typing import Dict, Any
from src.models.canonical_models import (
    CanonicalCourse, 
    CanonicalModule, 
    CanonicalCurriculumItem, 
    CanonicalAssessment, 
    CanonicalQuestion, 
    CanonicalAsset
)
from src.utils.content_normalizer import repair_text_encoding, normalize_lesson_content


class CanonicalNormalizer:
    """
    Normalizes a CanonicalCourse object to ensure its content and structural
    representation are deterministic and stable before hashing (fingerprinting).
    """
    def __init__(self, version: str = "1.0"):
        self.version = version

    def normalize(self, course: CanonicalCourse) -> CanonicalCourse:
        """
        Returns a normalized, deep-copied copy of the CanonicalCourse.
        """
        # Deep copy to ensure course immutability
        n_course = copy.deepcopy(course)
        
        # 1. Course title & description
        n_course.title = repair_text_encoding(n_course.title.strip())
        n_course.description = repair_text_encoding(n_course.description.strip())
        if n_course.course_code:
            n_course.course_code = n_course.course_code.strip()
            
        # 2. Normalize and sort assets by identifier
        normalized_assets = []
        for asset in n_course.assets:
            normalized_assets.append(self._normalize_asset(asset))
        n_course.assets = sorted(normalized_assets, key=lambda a: a.identifier)
        
        # 3. Normalize and sort assessments by identifier
        normalized_assessments = []
        for assessment in n_course.assessments:
            normalized_assessments.append(self._normalize_assessment(assessment))
        n_course.assessments = sorted(normalized_assessments, key=lambda a: a.identifier)
        
        # 4. Normalize modules and items
        normalized_modules = []
        for module in n_course.modules:
            normalized_modules.append(self._normalize_module(module))
        n_course.modules = sorted(normalized_modules, key=lambda m: (m.position, m.identifier))
        
        # 5. Clear volatile timestamps for hashing stability
        n_course.created_at = None
        n_course.updated_at = None
            
        return n_course

    def compute_content_fingerprint(self, normalized_course: CanonicalCourse) -> str:
        """
        Generates a SHA-256 content fingerprint for the course content,
        incorporating the normalizer version to prevent regression mismatches.
        """
        from enum import Enum
        def serialize_enum_default(obj):
            if isinstance(obj, Enum):
                return obj.value
            return str(obj)

        course_dict = asdict(normalized_course)
        # Serialize to deterministic JSON with sorted keys
        serialized_json = json.dumps(course_dict, sort_keys=True, default=serialize_enum_default)
        # Prefix with normalizer version to avoid cross-version hash collision
        payload = f"{self.version}:{serialized_json}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _normalize_asset(self, asset: CanonicalAsset) -> CanonicalAsset:
        asset.filename = repair_text_encoding(asset.filename.strip())
        if asset.url:
            asset.url = asset.url.strip()
        return asset

    def _normalize_assessment(self, assessment: CanonicalAssessment) -> CanonicalAssessment:
        assessment.title = repair_text_encoding(assessment.title.strip())
        assessment.description = normalize_lesson_content(assessment.description)
        
        # Normalize questions
        normalized_questions = []
        for question in assessment.questions:
            normalized_questions.append(self._normalize_question(question))
        assessment.questions = sorted(normalized_questions, key=lambda q: (q.position or 0, q.identifier))
        
        # Zero out volatile parameters
        assessment.due_at = None
        assessment.unlock_at = None
        assessment.lock_at = None
        return assessment

    def _normalize_question(self, question: CanonicalQuestion) -> CanonicalQuestion:
        question.text = normalize_lesson_content(question.text)
        if question.general_feedback:
            question.general_feedback = normalize_lesson_content(question.general_feedback)
        if question.correct_feedback:
            question.correct_feedback = normalize_lesson_content(question.correct_feedback)
        if question.incorrect_feedback:
            question.incorrect_feedback = normalize_lesson_content(question.incorrect_feedback)
            
        # Clean answer strings
        cleaned_answers = []
        for ans in question.answers:
            cleaned_ans = {}
            for k, v in ans.items():
                if isinstance(v, str):
                    cleaned_ans[k] = repair_text_encoding(v.strip())
                else:
                    cleaned_ans[k] = v
            cleaned_answers.append(cleaned_ans)
        question.answers = cleaned_answers
        return question

    def _normalize_module(self, module: CanonicalModule) -> CanonicalModule:
        module.title = repair_text_encoding(module.title.strip())
        module.description = repair_text_encoding(module.description.strip())
        module.prerequisite_module_ids = sorted(module.prerequisite_module_ids)
        module.unlock_at = None
        
        # Normalize and sort child items by position
        normalized_items = []
        for item in module.items:
            normalized_items.append(self._normalize_item(item))
        module.items = sorted(normalized_items, key=lambda i: i.position)
        return module

    def _normalize_item(self, item: CanonicalCurriculumItem) -> CanonicalCurriculumItem:
        item.title = repair_text_encoding(item.title.strip())
        if item.body:
            item.body = normalize_lesson_content(item.body)
        item.asset_refs = sorted(item.asset_refs)
        return item
