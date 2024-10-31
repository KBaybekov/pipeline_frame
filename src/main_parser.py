import argparse
import os
import importlib.util

def parse_args():
    # Парсим первый аргумент с путём к конфигу
    initial_args, remaining_args = parse_initial_args()

    # Добавляем project_path в оставшиеся аргументы
    remaining_args.extend(['--project_path', initial_args.project_path])

    # Загружаем и выполняем второй парсер, передаем остальные аргументы
    parse_cli_args = load_config_parser(initial_args.project_path)
    final_args = parse_cli_args()

    return final_args

def parse_initial_args():
    """
    Парсер для получения пути к конфигу.
    """
    parser = argparse.ArgumentParser(description="Initial argument parser")
    parser.add_argument('-pp', '--project_path', required=True, help="Путь для загрузки конфигурационных файлов")
    parser.add_argument('-db', '--debug', default=[], help="Уровень дебага (all, cmd_tpl, samples, cmds, errors, info)")
    
    # Используем parse_known_args, чтобы собрать только --project_path и передать остальные аргументы позже
    args, remaining_args = parser.parse_known_args()
    return args, remaining_args

def load_config_parser(project_path):
    """
    Импортирует второй парсер из указанного конфиг файла.
    """
    arg_parser = os.path.join(project_path, 'src/arg_parser.py')
    if not os.path.exists(arg_parser):
        raise FileNotFoundError(f"Парсер не найден: {arg_parser}")

    # Динамически загружаем второй парсер
    spec = importlib.util.spec_from_file_location("config_parser", arg_parser)
    config_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config_module)

    return config_module.parse_cli_args