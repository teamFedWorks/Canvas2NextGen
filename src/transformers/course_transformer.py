from typing import Dict, Any, List

class CourseTransformer:
    """
    Transforms parsed Canvas data into the MERN LMS MongoDB schema.
    """

    def transform(self, parsed_data: Dict[str, Any], university_id: str, author_id: str) -> Dict[str, Any]:
        """
        Maps intermediate representation to the final schema.
        """
        course = {
            "title": parsed_data.get("title", "Untitled Course"),
            "description": parsed_data.get("description", ""),
            "universityId": university_id,
            "authorId": author_id,
            "curriculum": []
        }

        for module in parsed_data.get("curriculum", []):
            transformed_module = {
                "title": module.get("title", ""),
                "summary": module.get("summary", ""),
                "items": []
            }

            for item in module.get("items", []):
                # Ensure each item has all required config fields
                lesson_item = {
                    "title": item.get("title", ""),
                    "type": item.get("type", "Lesson"),
                    "content": item.get("content", ""),
                    "videoUrl": item.get("videoUrl", ""),
                    "attachments": item.get("attachments", []),
                    "quizConfig": item.get("quizConfig", {}),
                    "assignmentConfig": item.get("assignmentConfig", {}),
                    "discussionConfig": item.get("discussionConfig", {})
                }
                transformed_module["items"].append(lesson_item)

            course["curriculum"].append(transformed_module)

        return course
