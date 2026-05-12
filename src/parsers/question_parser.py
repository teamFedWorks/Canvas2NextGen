"""
Question Parser - Parses Canvas quiz questions (QTI-compliant).

Handles all Canvas question types with proper QTI parsing.
"""

from pathlib import Path
from typing import List, Optional

from models.canvas_models import (
    CanvasQuestion,
    CanvasQuestionAnswer,
    QuestionType
)
from models.migration_report import MigrationError, ErrorSeverity
from utils.xml_utils import parse_xml_file, find_element, find_elements, get_element_text, get_element_attribute, get_inner_html
from utils.html_utils import sanitize_html


class QuestionParser:
    """
    Parses Canvas quiz questions from QTI XML.
    """
    
    def __init__(self, course_directory: Path):
        """
        Initialize question parser.
        
        Args:
            course_directory: Path to Canvas course export directory
        """
        self.course_directory = course_directory
        self.errors: List[MigrationError] = []
    
    def parse_questions_from_quiz(self, quiz_dir: Path) -> List[CanvasQuestion]:
        """
        Parse all questions from a quiz directory.
        
        Args:
            quiz_dir: Path to quiz directory
            
        Returns:
            List of CanvasQuestion objects
        """
        questions = []
        
        # Look for question XML files
        for xml_file in quiz_dir.glob("*.xml"):
            if xml_file.name in ("assessment_meta.xml", "assessment.xml", "assignment_settings.xml"):
                continue
            
            question = self.parse_question(xml_file)
            if question:
                questions.append(question)
        
        return questions
    
    def parse_question(self, question_file: Path) -> Optional[CanvasQuestion]:
        """
        Parse a single question XML file.
        
        Args:
            question_file: Path to question XML file
            
        Returns:
            CanvasQuestion object or None if parsing fails
        """
        try:
            root = parse_xml_file(question_file)
            if root is None:
                return None
            
            # Extract question metadata
            identifier = get_element_attribute(root, 'identifier', question_file.stem)
            title = get_element_text(find_element(root, './/title', {}), "Question")
            
            # Extract question text
            question_text = self._extract_question_text(root)
            
            # Determine question type
            question_type = self._determine_question_type(root)
            
            # Extract points
            points = self._extract_points(root)
            
            # Extract answers
            answers = self._extract_answers(root, question_type)
            
            # Extract feedback
            general_feedback = self._extract_feedback(root)
            
            question = CanvasQuestion(
                identifier=identifier,
                title=title,
                question_type=question_type,
                question_text=question_text,
                points_possible=points,
                answers=answers,
                general_feedback=general_feedback,
                source_file=str(question_file)
            )
            
            return question
            
        except Exception as e:
            self.errors.append(MigrationError(
                severity=ErrorSeverity.WARNING,
                error_type="QUESTION_PARSE_ERROR",
                message=f"Failed to parse question: {str(e)}",
                file_path=str(question_file)
            ))
            return None
    
    def _extract_question_text(self, root) -> str:
        """Extract question text/prompt"""
        # Try itemBody first (QTI standard)
        item_body = find_element(root, './/itemBody', {})
        if item_body is not None:
            return sanitize_html(get_inner_html(item_body))
        
        # Fallback to presentation/material
        material = find_element(root, './/presentation//material', {})
        if material is not None:
            return sanitize_html(get_inner_html(material))
        
        # Fallback to question_text
        question_text = find_element(root, './/question_text', {})
        if question_text is not None:
            return sanitize_html(get_element_text(question_text, ""))
        
        return ""
    
    def _determine_question_type(self, root) -> QuestionType:
        """Determine question type from XML structure"""
        # Check for question_type element
        type_elem = find_element(root, './/question_type', {})
        if type_elem is not None:
            type_text = get_element_text(type_elem, "").lower()
            
            # Map to QuestionType enum
            type_mapping = {
                'multiple_choice_question': QuestionType.MULTIPLE_CHOICE,
                'true_false_question': QuestionType.TRUE_FALSE,
                'essay_question': QuestionType.ESSAY,
                'short_answer_question': QuestionType.SHORT_ANSWER,
                'fill_in_multiple_blanks_question': QuestionType.FILL_IN_BLANK,
                'matching_question': QuestionType.MATCHING,
                'numerical_question': QuestionType.NUMERICAL,
                'calculated_question': QuestionType.CALCULATED,
                'multiple_answers_question': QuestionType.MULTIPLE_ANSWERS,
                'file_upload_question': QuestionType.FILE_UPLOAD,
                'text_only_question': QuestionType.TEXT_ONLY,
                'ordering_question': QuestionType.ORDERING,
            }
            
            return type_mapping.get(type_text, QuestionType.ESSAY)
        
        # Infer from response type
        response_decl = find_element(root, './/responseDeclaration', {})
        if response_decl is not None:
            cardinality = get_element_attribute(response_decl, 'cardinality', 'single')
            if cardinality == 'multiple':
                return QuestionType.MULTIPLE_ANSWERS
            elif cardinality == 'single':
                return QuestionType.MULTIPLE_CHOICE
        
        return QuestionType.ESSAY
    
    def _extract_points(self, root) -> float:
        """Extract points possible"""
        # Try maxScore
        max_score = find_element(root, './/maxScore', {})
        if max_score is not None:
            try:
                return float(get_element_text(max_score, "1"))
            except ValueError:
                pass
        
        # Try points_possible
        points_elem = find_element(root, './/points_possible', {})
        if points_elem is not None:
            try:
                return float(get_element_text(points_elem, "1"))
            except ValueError:
                pass
        
        return 1.0
    
    def _extract_answers(self, root, question_type: QuestionType) -> List[CanvasQuestionAnswer]:
        """Extract answer choices"""
        answers = []
        
        # For essay/file upload questions, no answers
        if question_type in (QuestionType.ESSAY, QuestionType.FILE_UPLOAD, QuestionType.TEXT_ONLY):
            return answers
        
        # Find response choices (QTI 2.x)
        choices = find_elements(root, './/simpleChoice', {})
        
        # If not found, try response_lid/render_choice (QTI 1.2 - common in Canvas)
        if not choices:
            choices = find_elements(root, './/response_lid//response_label', {})
        
        # If still not found, try response_choice
        if not choices:
            choices = find_elements(root, './/response_choice', {})
        
        for choice in choices:
            answer_id = get_element_attribute(choice, 'identifier') or get_element_attribute(choice, 'ident', '')
            
            # Extract text from material/mattext (QTI 1.2) or inner HTML (QTI 2.x)
            mattext = find_element(choice, './/mattext', {})
            if mattext is not None:
                answer_text = sanitize_html(get_element_text(mattext, ""))
            else:
                answer_text = sanitize_html(get_inner_html(choice))
            
            if not answer_text and not answer_id:
                continue

            # Determine if correct (weight = 100)
            weight = self._get_answer_weight(root, answer_id)
            
            answer = CanvasQuestionAnswer(
                id=answer_id,
                text=answer_text or answer_id,
                weight=weight
            )
            answers.append(answer)
        
        return answers
    
    def _get_answer_weight(self, root, answer_id: str) -> float:
        """Get weight/score for an answer"""
        # Look in responseProcessing for correct answer (QTI 2.x)
        correct_responses = find_elements(root, './/correctResponse//value', {})
        for correct_resp in correct_responses:
            if get_element_text(correct_resp, '') == answer_id:
                return 100.0
        
        # Look in respcondition (QTI 1.2)
        resp_conditions = find_elements(root, './/respcondition', {})
        for cond in resp_conditions:
            setvar = find_element(cond, './/setvar', {})
            if setvar is not None and get_element_text(setvar, '0') == '100':
                varequal = find_element(cond, './/varequal', {})
                if varequal is not None and get_element_text(varequal, '') == answer_id:
                    return 100.0
        
        return 0.0
    
    def _extract_feedback(self, root) -> Optional[str]:
        """Extract general feedback"""
        feedback_elem = find_element(root, './/generalFeedback', {})
        if feedback_elem is not None:
            return sanitize_html(get_inner_html(feedback_elem))
        
        # Try modalFeedback
        modal_feedback = find_element(root, './/modalFeedback', {})
        if modal_feedback is not None:
            return sanitize_html(get_inner_html(modal_feedback))
        
        return None
