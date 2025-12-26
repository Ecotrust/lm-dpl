#!/usr/bin/env python3
"""
Tests CLI functionality.
"""

import pytest
from unittest.mock import patch
from io import StringIO

from lm_dpl.cli import main, run_parcels, run_soil


class TestCLI:
    """Test cases for the CLI functionality."""

    def test_main_no_arguments(self):
        """Test that main returns error code when no arguments are provided."""
        with patch("sys.argv", ["lm-dpl"]), pytest.raises(SystemExit):
            main()  # Should exit with error due to required subcommand

    def test_main_help(self):
        """Test that help is displayed when no command is provided."""
        with (
            patch("sys.argv", ["lm-dpl", "--help"]),
            patch("sys.stdout", new_callable=StringIO) as mock_stdout,
        ):
            try:
                main()
            except SystemExit:
                pass
            help_output = mock_stdout.getvalue()
            assert "Landmapper Data Pipeline Library (lm-dpl)" in help_output

    def test_main_parcels_command(self):
        """Test parcels command execution."""
        with (
            patch("sys.argv", ["lm-dpl", "parcels", "oregon"]),
            patch("lm_dpl.cli.run_parcels", return_value=0) as mock_run_parcels,
        ):
            result = main()
            mock_run_parcels.assert_called_once_with("oregon", None, None)
            assert result == 0

    def test_main_parcels_command_with_layer(self):
        """Test parcels command execution with layer argument."""
        with (
            patch("sys.argv", ["lm-dpl", "parcels", "--layer", "fpd", "oregon"]),
            patch("lm_dpl.cli.run_parcels", return_value=0) as mock_run_parcels,
        ):
            result = main()
            mock_run_parcels.assert_called_once_with("oregon", None, ["fpd"])
            assert result == 0

    def test_main_parcels_command_with_multiple_layers(self):
        """Test parcels command execution with multiple layer arguments."""
        with (
            patch(
                "sys.argv",
                ["lm-dpl", "parcels", "--layer", "fpd", "--layer", "plss1", "oregon"],
            ),
            patch("lm_dpl.cli.run_parcels", return_value=0) as mock_run_parcels,
        ):
            result = main()
            mock_run_parcels.assert_called_once_with("oregon", None, ["fpd", "plss1"])
            assert result == 0

    def test_main_parcels_command_with_short_layer_flag(self):
        """Test parcels command execution with short layer flag."""
        with (
            patch(
                "sys.argv", ["lm-dpl", "parcels", "-l", "fpd", "-l", "plss2", "oregon"]
            ),
            patch("lm_dpl.cli.run_parcels", return_value=0) as mock_run_parcels,
        ):
            result = main()
            mock_run_parcels.assert_called_once_with("oregon", None, ["fpd", "plss2"])
            assert result == 0

    def test_main_soil_command(self):
        """Test soil command execution."""
        with (
            patch("sys.argv", ["lm-dpl", "soil", "CA"]),
            patch("lm_dpl.cli.run_soil", return_value=0) as mock_run_soil,
        ):
            result = main()
            mock_run_soil.assert_called_once_with("CA", None)
            assert result == 0

    def test_run_parcels_success(self):
        """Test successful parcel processing."""
        with patch("lm_dpl.parcels.processor.ParcelProcessor") as mock_processor_class:
            mock_processor_instance = mock_processor_class.return_value
            result = run_parcels("oregon")
            mock_processor_class.assert_called_once_with("oregon", config_path=None)
            mock_processor_instance.fetch.assert_called_once()
            assert result == 0

    def test_run_parcels_failure(self):
        """Test parcel processing failure."""
        with patch(
            "lm_dpl.parcels.processor.ParcelProcessor",
            side_effect=Exception("Test error"),
        ):
            result = run_parcels("oregon")
            assert result == 1

    def test_run_soil_success(self):
        """Test successful soil processing."""
        with patch("lm_dpl.soil.processor.main") as mock_soil_main:
            result = run_soil("CA")
            mock_soil_main.assert_called_once_with("ca", None)
            assert result == 0

    def test_run_soil_failure(self):
        """Test soil processing failure."""
        with patch(
            "lm_dpl.soil.processor.main", side_effect=Exception("Test error")
        ) as mock_soil_main:
            result = run_soil("CA")
            mock_soil_main.assert_called_once_with("ca", None)
            assert result == 1

    def test_unknown_command(self):
        """Test handling of unknown command."""
        with (
            patch("sys.argv", ["lm-dpl", "unknown", "state"]),
            pytest.raises(SystemExit),
        ):
            main()  # Should exit with error due to invalid command choice


class TestCLIArgumentParsing:
    """Test cases for CLI argument parsing."""

    def test_parcels_parser(self):
        """Test parcels subparser configuration."""
        with (
            patch("sys.argv", ["lm-dpl", "parcels", "--help"]),
            patch("sys.stdout", new_callable=StringIO) as mock_stdout,
        ):
            try:
                main()
            except SystemExit:
                pass
            help_output = mock_stdout.getvalue()
            assert "state" in help_output
            assert "--layer" in help_output
            assert "-l" in help_output
            assert "Process specific layer(s)" in help_output

    def test_soil_parser(self):
        """Test soil subparser configuration."""
        with (
            patch("sys.argv", ["lm-dpl", "soil", "--help"]),
            patch("sys.stdout", new_callable=StringIO) as mock_stdout,
        ):
            try:
                main()
            except SystemExit:
                pass
            help_output = mock_stdout.getvalue()
            assert (
                "State name or abbreviation (e.g., oregon, OR, washington, WA)"
                in help_output
            )
            assert "state" in help_output

    def test_global_arguments(self):
        """Test global argument parsing."""
        with (
            patch("sys.argv", ["lm-dpl", "--help"]),
            patch("sys.stdout", new_callable=StringIO) as mock_stdout,
        ):
            try:
                main()
            except SystemExit:
                pass
            help_output = mock_stdout.getvalue()
            assert "--verbose" in help_output
            assert "Examples:" in help_output


if __name__ == "__main__":
    pytest.main([__file__])
