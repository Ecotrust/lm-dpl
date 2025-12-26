#!/usr/bin/env python3
"""
Tests CLI functionality.
"""

import pytest
from unittest.mock import patch
from io import StringIO

from lm_dpl.cli import main, run_fetch, run_process


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

    def test_main_fetch_command(self):
        """Test fetch command execution."""
        with (
            patch("sys.argv", ["lm-dpl", "fetch", "oregon"]),
            patch("lm_dpl.cli.run_fetch", return_value=0) as mock_run_fetch,
        ):
            result = main()
            # args.layer is None by default in argparse if not provided
            mock_run_fetch.assert_called_once_with("oregon", layers=None, config_path=None, overwrite=False)
            assert result == 0

    def test_main_fetch_command_with_layer(self):
        """Test fetch command execution with layer argument."""
        with (
            patch("sys.argv", ["lm-dpl", "fetch", "--layer", "fpd", "oregon"]),
            patch("lm_dpl.cli.run_fetch", return_value=0) as mock_run_fetch,
        ):
            result = main()
            mock_run_fetch.assert_called_once_with("oregon", layers=["fpd"], config_path=None, overwrite=False)
            assert result == 0

    def test_main_fetch_command_with_multiple_layers(self):
        """Test fetch command execution with multiple layer arguments."""
        with (
            patch(
                "sys.argv",
                ["lm-dpl", "fetch", "--layer", "fpd", "--layer", "plss1", "oregon"],
            ),
            patch("lm_dpl.cli.run_fetch", return_value=0) as mock_run_fetch,
        ):
            result = main()
            mock_run_fetch.assert_called_once_with(
                "oregon", layers=["fpd", "plss1"], config_path=None, overwrite=False
            )
            assert result == 0

    def test_main_fetch_command_with_short_layer_flag(self):
        """Test fetch command execution with short layer flag."""
        with (
            patch(
                "sys.argv", ["lm-dpl", "fetch", "-l", "fpd", "-l", "plss2", "oregon"]
            ),
            patch("lm_dpl.cli.run_fetch", return_value=0) as mock_run_fetch,
        ):
            result = main()
            mock_run_fetch.assert_called_once_with(
                "oregon", layers=["fpd", "plss2"], config_path=None, overwrite=False
            )
            assert result == 0

    def test_main_fetch_soil(self):
        """Test fetch command execution for soil layer."""
        with (
            patch("sys.argv", ["lm-dpl", "fetch", "--layer", "soil", "CA"]),
            patch("lm_dpl.cli.run_fetch", return_value=0) as mock_run_fetch,
        ):
            result = main()
            mock_run_fetch.assert_called_once_with(
                "CA", layers=["soil"], config_path=None, overwrite=False
            )
            assert result == 0
            
    def test_main_process_command(self):
        """Test process command execution."""
        with (
            patch("sys.argv", ["lm-dpl", "process", "--table", "taxlots", "--state", "OR"]),
            patch("lm_dpl.cli.run_process", return_value=0) as mock_run_process,
        ):
            result = main()
            mock_run_process.assert_called_once_with("taxlots", "OR")
            assert result == 0

    def test_run_fetch_all_success(self):
        """Test successful fetch of all layers (parcels processor)."""
        with patch("lm_dpl.parcels.processor.ParcelProcessor") as mock_processor_class:
            mock_processor_instance = mock_processor_class.return_value
            result = run_fetch("oregon")
            mock_processor_class.assert_called_once_with("oregon", config_path=None)
            mock_processor_instance.fetch.assert_called_once_with(overwrite=False)
            assert result == 0

    def test_run_fetch_failure(self):
        """Test fetch failure."""
        with patch(
            "lm_dpl.parcels.processor.ParcelProcessor",
            side_effect=Exception("Test error"),
        ):
            result = run_fetch("oregon")
            assert result == 1

    def test_run_fetch_soil_success(self):
        """Test successful soil fetch."""
        with patch("lm_dpl.soil.processor.main") as mock_soil_main:
            # When layers=['soil'], run_fetch should call soil_main
            # Note: run_fetch modifies the layers list in place, so we pass a copy or fresh list
            result = run_fetch("CA", layers=["soil"])
            # soil_main expects (state_abbr, config_path)
            # CA normalizes to ca in soil_main call? normalize_state("CA", to="abbr") -> "ca" (if "CA" not in map, assumes it's abbr? No wait.)
            # normalize_state implementation:
            # "oregon" -> "or", "washington" -> "wa".
            # "CA" lower is "ca". Not in name_to_abbr map keys ("oregon", "washington").
            # returns "ca".
            mock_soil_main.assert_called_once_with("ca", None)
            assert result == 0

    def test_run_fetch_soil_failure(self):
        """Test soil fetch failure."""
        with patch(
            "lm_dpl.soil.processor.main", side_effect=Exception("Test error")
        ) as mock_soil_main:
            result = run_fetch("CA", layers=["soil"])
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

    def test_fetch_parser(self):
        """Test fetch subparser configuration."""
        with (
            patch("sys.argv", ["lm-dpl", "fetch", "--help"]),
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
            assert "Fetch specific layer(s)" in help_output

    def test_process_parser(self):
        """Test process subparser configuration."""
        with (
            patch("sys.argv", ["lm-dpl", "process", "--help"]),
            patch("sys.stdout", new_callable=StringIO) as mock_stdout,
        ):
            try:
                main()
            except SystemExit:
                pass
            help_output = mock_stdout.getvalue()
            assert "--table" in help_output
            assert "--state" in help_output
            assert "taxlots" in help_output  # Choices are shown

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
