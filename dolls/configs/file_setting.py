# !/usr/bin/env Python3
# -*- coding: utf-8 -*-
# @Author   : zhangzhanqi
# @FILE     : file_setting.py
# @Time     : 2022/4/25 17:59
# @Desc     :
from pathlib import Path
from typing import Any, Dict, Union, Optional

from pydantic import BaseSettings
from pydantic.env_settings import SettingsError


def read_yaml_file(yaml_file: Path,
                   encoding: str,
                   case_sensitive: bool) -> Dict[str, Any]:
    if yaml_file.exists():
        import yaml
        values = yaml.safe_load(yaml_file.read_text(encoding) or '{}')
    else:
        values = {}
    if case_sensitive:
        return values
    else:
        return {k.lower(): v for k, v in values.items()}


def read_json_file(json_file: Path,
                   encoding: str,
                   case_sensitive: bool) -> Dict[str, Any]:
    if json_file.exists():
        try:
            import ujson as json
        except ImportError:
            import json
        values = json.loads(json_file.read_text(encoding) or '{}')
    else:
        values = {}
    if case_sensitive:
        return values
    else:
        return {k.lower(): v for k, v in values.items()}


class BaseFileSettings(BaseSettings):
    _AUTHOR = "ZhangZhanQi"

    class Config:
        conf_file = None

        @classmethod
        def customise_sources(
                cls,
                init_settings,
                env_settings,
                file_secret_settings,
        ):
            return (
                init_settings,
                FileSettingsSource(cls.conf_file),
                env_settings,
                file_secret_settings,
            )


class FileSettingsSource:
    __slots__ = ('conf_file', 'conf_file_encoding')

    def __init__(self, conf_file: Union[Path, str, None], conf_file_encoding: Optional[str] = 'utf-8'):
        self.conf_file: Union[Path, str, None] = conf_file
        self.conf_file_encoding: Optional[str] = conf_file_encoding

    def __call__(self, settings: BaseSettings) -> Dict[str, Any]:
        """
        Build environment variables suitable for passing to the Model.
        """
        d: Dict[str, Optional[str]] = {}

        conf_vars = {}
        if self.conf_file is not None:
            conf_path = Path(self.conf_file).expanduser()
            if conf_path.is_file():
                conf_vars = {
                    **read_yaml_file(
                        conf_path, encoding=self.conf_file_encoding, case_sensitive=settings.__config__.case_sensitive
                    ),
                }

        for field in settings.__fields__.values():
            conf_val: Optional[str] = None
            for conf_name in field.field_info.extra['env_names']:
                conf_val = conf_vars.get(conf_name)
                if conf_val is not None:
                    break

            if conf_val is None:
                continue

            if field.is_complex():
                try:
                    conf_val = settings.__config__.json_loads(conf_val)  # type: ignore
                except ValueError as e:
                    raise SettingsError(f'error parsing JSON for "{conf_name}"') from e
            d[field.alias] = conf_val
        return d

    def __repr__(self) -> str:
        return f'FileSettingsSource(conf_file={self.conf_file!r}, conf_file_encoding={self.conf_file_encoding!r})'
