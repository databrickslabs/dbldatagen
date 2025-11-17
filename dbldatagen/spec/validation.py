class ValidationResult:
    """Container for validation results that collects errors and warnings during spec validation.

    This class accumulates validation issues found while checking a DatagenSpec configuration.
    It distinguishes between errors (which prevent data generation) and warnings (which
    indicate potential issues but don't block generation).

    .. note::
        Validation passes if there are no errors, even if warnings are present
    """

    def __init__(self) -> None:
        """Initialize an empty ValidationResult with no errors or warnings."""
        self.errors: list[str] = []
        self.warnings: list[str] = []

    def add_error(self, message: str) -> None:
        """Add an error message to the validation results.

        Errors indicate critical issues that will prevent successful data generation.

        :param message: Descriptive error message explaining the validation failure
        """
        self.errors.append(message)

    def add_warning(self, message: str) -> None:
        """Add a warning message to the validation results.

        Warnings indicate potential issues or non-optimal configurations that may affect
        data generation but won't prevent it from completing.

        :param message: Descriptive warning message explaining the potential issue
        """
        self.warnings.append(message)

    def is_valid(self) -> bool:
        """Check if validation passed without errors.

        :returns: True if there are no errors (warnings are allowed), False otherwise
        """
        return len(self.errors) == 0

    def __str__(self) -> str:
        """Generate a formatted string representation of all validation results.

        :returns: Multi-line string containing formatted errors and warnings with counts
        """
        lines = []
        if self.is_valid():
            lines.append("✓ Validation passed successfully")
        else:
            lines.append("✗ Validation failed")

        if self.errors:
            lines.append(f"\nErrors ({len(self.errors)}):")
            for i, error in enumerate(self.errors, 1):
                lines.append(f"  {i}. {error}")

        if self.warnings:
            lines.append(f"\nWarnings ({len(self.warnings)}):")
            for i, warning in enumerate(self.warnings, 1):
                lines.append(f"  {i}. {warning}")

        return "\n".join(lines)
