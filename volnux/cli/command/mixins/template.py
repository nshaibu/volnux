from typing import Any, Dict
from pathlib import Path


class TemplateMixin:
    """
    Mixin for handling template rendering and parameter substitution.
    """

    @classmethod
    def _get_rendered_template(cls, template_name: str, params: Dict[str, Any]) -> str:
        """Get and render template content with provided parameters.

        Args:
            template_name: Name of the template file (e.g., 'workflow.txt')
            params: Dictionary of parameters to substitute in the template

        Returns:
            Rendered template content as a string

        Raises:
            FileNotFoundError: If the template file doesn't exist,
            ValueError: If template_name is empty or contains path traversal
            KeyError: If required template parameters are missing
            Exception: For other rendering errors
        """
        if not template_name or not template_name.strip():
            raise ValueError("Template name cannot be empty")

        if ".." in template_name or template_name.startswith("/"):
            raise ValueError(f"Invalid template name: {template_name}")

        # Construct a template path
        current_dir = Path(__file__).parent
        templates_dir = current_dir / "builtins" / "templates"
        template_file_path = templates_dir / template_name

        try:
            template_file_path = template_file_path.resolve()
            templates_dir = templates_dir.resolve()
            if not str(template_file_path).startswith(str(templates_dir)):
                raise ValueError(
                    f"Template path outside allowed directory: {template_name}"
                )
        except (OSError, RuntimeError) as e:
            raise ValueError(f"Invalid template path: {template_name}") from e

        if not template_file_path.exists():
            raise FileNotFoundError(
                f"Template file not found: {template_name} "
                f"(expected at: {template_file_path})"
            )

        if not template_file_path.is_file():
            raise ValueError(f"Template path is not a file: {template_name}")

        try:
            content = template_file_path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as e:
            raise IOError(f"Failed to read template file {template_name}: {e}") from e

        # Render template with parameters
        try:
            return content.format(**params)
        except KeyError as e:
            raise KeyError(
                f"Missing required parameter in template {template_name}: {e}"
            ) from e
        except (ValueError, IndexError) as e:
            raise Exception(f"Error rendering template {template_name}: {e}") from e
