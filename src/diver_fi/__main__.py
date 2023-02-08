"""
Arquivo executavel 
"""

import importlib
from pathlib import Path

from kedro.framework.cli.utils import KedroCliError, load_entry_points
from kedro.framework.project import configure_project

def _find_run_command(package_name):
    try:
        project_cli = importlib.import_module(f"{package_name}.cli")
        # falha elegantemente caso não exista cli.py
    except ModuleNotFoundError as exc:
        if f"{package_name}.cli" not in str(exc):
            raise
        plugins = load_entry_points("project")
        run = _find_run_command_in_plugins(plugins) if plugins else None
        if run:
            # usa o run caso haja nos plugins
            return run
        # importa o run diretamente da bibloteca
        from kedro.framework.cli.project import run

        return run
    # falha seriamente se cli.py existe, mas não há .cli nele
    if not hasattr(project_cli, "cli"):
        raise KedroCliError(f"Cannot load commands from {package_name}.cli")
    return project_cli.run


def _find_run_command_in_plugins(plugins):
    for group in plugins:
        if "run" in group.commands:
            return group.commands["run"]


def main(*args, **kwargs):
    package_name = Path(__file__).parent.name
    configure_project(package_name)
    run = _find_run_command(package_name)
    run(*args, **kwargs)


if __name__ == "__main__":
    main()