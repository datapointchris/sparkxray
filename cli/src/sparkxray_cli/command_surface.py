"""Every command the assembled `sparkxray` tree holds, read from the tree itself, as `capabilities show` lists them."""

from __future__ import annotations

import re
from typing import Any

import typer

# A command with one of these verbs can write a file the caller names. Every command may write sparkxray's cache.
_WRITING_VERBS = ('write',)
_TYPES = {
    'str': 'string',
    'text': 'string',
    'int': 'integer',
    'int range': 'integer',
    'float': 'number',
    'float range': 'number',
    'boolean': 'boolean',
    'choice': 'choice',
    'path': 'path',
    'file': 'path',
}
_USAGE = 'a usage error, or an input sparkxray refused; the error code names which'


def command_surface(ctx: typer.Context) -> list[dict[str, Any]]:
    """Every command under the root of `ctx`, in the order its help lists them, as `schemas/capabilities.json` describes one."""
    commands: list[dict[str, Any]] = []
    _walk(ctx, ctx.find_root().command, (), commands)
    return commands


def _walk(ctx: typer.Context, group: Any, path: tuple[str, ...], commands: list[dict[str, Any]]) -> None:
    for name in group.list_commands(ctx):
        command = group.get_command(ctx, name)
        if hasattr(command, 'list_commands'):
            _walk(ctx, command, (*path, name), commands)
        else:
            commands.append(_command((*path, name), command))


def _command(path: tuple[str, ...], command: Any) -> dict[str, Any]:
    exit_codes = {'0': 'it ran', '2': _USAGE}
    # Commands gate different findings, so each one's --fail-on help states its own gate.
    if any('--fail-on' in parameter.opts for parameter in command.params):
        exit_codes |= {
            '0': 'it ran, and passed the gate its --fail-on help states',
            '3': 'it failed the gate its --fail-on help states, or a log could not be read',
        }
    elif any(_names_a_log(parameter) for parameter in command.params):
        exit_codes['3'] = 'the log could not be read'
    return {
        'path': list(path),
        'verb': path[-1],
        'reads': path[-1] not in _WRITING_VERBS,
        'summary': _first_paragraph(command.help),
        'arguments': [
            {'name': parameter.metavar or parameter.name.upper(), 'required': parameter.required, 'help': _first_paragraph(parameter.help)}
            for parameter in command.params
            if parameter.param_type_name == 'argument'
        ],
        'flags': [_flag(parameter) for parameter in command.params if parameter.param_type_name == 'option'],
        'exit_codes': exit_codes,
    }


def _names_a_log(parameter: Any) -> bool:
    """Whether a parameter takes a log location, which `LogArgument` and every other log parameter show as `LOG` or `LOG_<role>`."""
    metavar = parameter.metavar or ''
    return metavar == 'LOG' or metavar.startswith('LOG_')


def flag_names(option: Any) -> list[str]:
    """An option's names, its first long one first, as capabilities and a refusal's `field` name it."""
    return sorted(option.opts, key=lambda name: (not name.startswith('--'), name))


def _flag(option: Any) -> dict[str, Any]:
    names = flag_names(option)
    choices = getattr(option.type, 'choices', None)
    return {
        'name': names[0],
        'aliases': names[1:],
        'type': _TYPES.get(option.type.name, option.type.name),
        'choices': list(choices) if choices is not None else None,
        'multiple': option.multiple,
        'required': option.required,
        'default': _plain(option.default),
        'help': _first_paragraph(option.help),
    }


def _plain(value: Any) -> Any:
    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, list | tuple):
        return [_plain(item) for item in value]
    return str(value)


def _first_paragraph(text: str | None) -> str | None:
    if not text:
        return None
    return re.sub(r'\s+', ' ', text.strip().split('\n\n')[0])
