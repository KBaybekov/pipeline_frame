import yaml
import os
import time
from datetime import datetime
import subprocess

def load_yaml(file_path:str, critical:bool = False, subsection:str = ''):
    """
    Универсальная функция для загрузки данных из YAML-файла.

    :param file_path: Путь к YAML-файлу. Ожидается строка, указывающая на местоположение файла с данными.
    :param critical: Возвращает ошибку, если файл не найден
    :param subsection: Опциональный параметр. Если передан, функция вернёт только данные из указанной
                       секции (например, конкретного этапа пайплайна). Если пусто, возвращаются все данные.
                       По умолчанию - пустая строка, что означает возврат всего содержимого файла.
    
    :return: Возвращает словарь с данными из YAML-файла. Если указан параметр subsection и он присутствует
             в YAML, возвращается соответствующая секция, иначе — всё содержимое файла.
    """
    # Открываем YAML-файл для чтения
    try:
        with open(file_path, 'r') as file:
            data = yaml.safe_load(file)  # Загружаем содержимое файла в словарь с помощью safe_load
        
        # Если subsection не указан, возвращаем весь YAML-файл
        if subsection == '':
            return data
        else:
            # Если subsection указан и существует в файле, возвращаем только эту секцию
            if subsection  in data.keys():
                return data[subsection]
            else:
                raise ValueError(f"Раздел '{subsection}' не найден в {file_path}")
    except FileNotFoundError as e:
        # Если файл не найден, возвращаем пустой словарь или ошибку, если данные необходимы для дальнейшей работы
        if critical:
            raise FileNotFoundError(f"Не найден: {file_path}")
        return {}


def save_yaml(filename, path, data):
    """
    Сохраняет словарь в файл в формате YAML.
    
    :param filename: Имя файла для сохранения (например, 'config.yaml')
    :param path: Путь к директории, где будет сохранён файл
    :param data: Словарь с данными, которые нужно сохранить в YAML
    """
    # Полный путь к файлу
    file_path = f'{path}{filename}.yaml'

    # Записываем данные в YAML-файл
    with open(file_path, 'w') as yaml_file:
        yaml.dump(data, yaml_file, default_flow_style=False, sort_keys=False)


def update_yaml(file_path: str, new_data: dict):
    """
    Обновляет YAML-файл, считывая и перезаписывая его новыми данными
    :param file_path: путь к файлу в виде строки
    :param new_data: данные в виде словаря
    """
    # Шаг 1: Загрузить текущие данные из YAML
    try:
        with open(file_path, 'r') as file:
            current_data = yaml.safe_load(file) or {}
    except FileNotFoundError:
        current_data = {}  # Если файл не найден, создаём пустой словарь

    # Шаг 2: Обновить значения существующих ключей
    for key, value in new_data.items():
        if key in current_data:
            current_data[key].update(value)  # Обновляем только значения
        else:
            current_data[key] = value  # Добавляем новый ключ, если его нет

    # Шаг 3: Записать обновлённые данные обратно в YAML
    with open(file_path, 'w') as file:
        yaml.dump(current_data, file, default_flow_style=False)


def load_templates(path: str, required_files:list) -> dict:
    """
    Загружает конфигурационные файлы из указанной директории.
    Выдаёт ошибку в случае отсутствия файла или проблем с его загрузкой.

    :param path: Путь к директории, где хранятся конфигурационные YAML-файлы.
    :param required_files: Конфигурационные YAML-файлы.
    """
    loaded_configs = {}

    # Проходим по списку обязательных файлов
    for req_file in required_files:
        file_path = os.path.join(path, f'{req_file}.yaml')
        
        # Проверяем наличие файла
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"Файл {req_file}.yaml не найден в директории {path}")
        
        # Пытаемся загрузить файл
        try:
            with open(file_path, 'r') as f:
                loaded_configs[req_file] = yaml.safe_load(f)  # Загружаем содержимое файла
        except yaml.YAMLError as e:
            raise ValueError(f"Ошибка загрузки файла {req_file}.yaml: {e}")
    
    # Возвращаем загруженные конфигурации
    return loaded_configs    

        
def get_paths(folders: dict, input_dir: str, output_dir: str) -> dict:
    """
    Создаёт словарь с путями для всех директорий, указанных в словаре 'folders',
    где для каждой директории указан путь относительно 'input_dir' или 'output_dir'.

    :param folders: Словарь с поддиректориями для 'input_dir' и 'output_dir'
    :param input_dir: Базовая директория для 'input_dir'
    :param output_dir: Базовая директория для 'output_dir'
    :return: Словарь с абсолютными путями к каждой директории
    """
    # Формируем словарь директорий с полными путями
    folders_with_paths = {
        # Добавляем 'input': input_dir
        'input_dir': input_dir,
        'output_dir': output_dir,
        # Проходим по всем директориям в 'input_dir' и добавляем базовый путь 'input_dir'
        **{key: os.path.join(input_dir, f'{value}/') for key, value in (folders.get('input_dir') or {}).items()},
        # Проходим по всем директориям в 'output_dir' и добавляем базовый путь 'output_dir'
        **{key: os.path.join(output_dir, f'{value}/') for key, value in (folders.get('output_dir') or {}).items()}
    }

    return folders_with_paths


def generate_cmd_data(args:dict, folders:dict,
                        executables:dict, 
                        filenames:dict, commands:dict,
                        cmds_dict:dict, samples:list):
    """
    Генерирует команды для каждого образца на основе аргументов, файлов и шаблонов команд.
    
    :param args: Аргументы пайплайна (содержат параметры запуска).
    :param folders: Словарь с директориями (входные и выходные директории).
    :param executables: Словарь с исполняемыми файлами.
    :param filenames: Словарь с шаблонами файлов для текущего образца.
    :param commands: Шаблоны команд для выполнения.
    :param cmds_dict: Список команд, которые нужно сгенерировать.
    :param samples: Список образцов для обработки.
    :return: Словарь с командами для каждого образца.
    """
    # Объединяем все переменные в один словарь для подстановки в eval()
    context = {
            'programms': executables,
            'folders': folders,
            'args': args,
            'os': os  # Добавляем os в контекст, чтобы os.path был доступен
        }

    cmd_data = {}
    # Создаём набор команд, которые выполнятся однократно перед прогоном по образцам
    cmd_data['before_batch'] = generate_commands(context=context, cmd_list=cmds_dict['before_batch'], commands=commands)
    # Создаём набор команд для каждого образца
    cmd_data['batch'] = {}
    for sample in samples:
        sample = sample.replace('//', '/')
        # Генерируем файлы для конкретного образца
        sample_filenames = generate_sample_filenames(sample=sample, folders=folders, filenames=filenames)
        # Объединяем все переменные в один словарь для подстановки в eval()
        context['filenames'] = sample_filenames
        # Генерируем команды для образцов на основе аргументов, файлов и шаблонов команд
        cmds = generate_commands(context=context, cmd_list=cmds_dict['sample_level'], commands=commands)
        
        # Добавляем сгенерированные команды в словарь для текущего образца
        cmd_data['batch'][sample_filenames['basename']] = cmds

    # Создаём набор команд, которые выполнятся однократно после прогона по образцам
    cmd_data['after_batch'] = generate_commands(context=context, cmd_list=cmds_dict['after_batch'], commands=commands)

    return cmd_data


def generate_sample_list(in_samples: list, ex_samples: list,
                         input_dir: str, extensions: tuple, subfolders:bool=False) -> list:
    """
    В зависимости от значения subfolders возвращает список файлов с указанным расширением только из указанной директории либо \
    и из подпапок тоже. При наличии включающих/исключающих паттернов фильтрует список образцов по ним.\n
    Выдаёт ошибку, если итоговый список пустой.

    :param in_samples: Список образцов, которые нужно включить.
    :param ex_samples: Список образцов, которые нужно исключить.
    :param input_dir: Директория, где искать файлы.
    :param extensions: Расширения файлов для поиска.
    :param subdirs: поиск в подпапках.
    :return: Список путей к файлам.
    """
    if subfolders:
        # Ищем все файлы в дереве папок с указанными расширениями
        samples = get_samples_in_dir_tree(dir=input_dir, extensions=extensions)
    else:
        # Ищем все файлы в одной папке с указанными расширениями
        samples = get_samples_in_dir(dir=input_dir, extensions=extensions)
    found_samples = len(samples)
    # Если список включающих образцов непустой, фильтруем по нему
    if in_samples:
        samples = [s for s in samples if 
                 any(inclusion in os.path.basename(s) for inclusion in in_samples)]
    # Если список исключающих образцов непустой, фильтруем по нему перед выдачей итогового списка образцов
    if ex_samples:
        samples = [s for s in samples if 
                 not any(exclusion in os.path.basename(s) for exclusion in ex_samples)]
    len_samples = len(samples)
    # Если итоговый список пустой, выдаём ошибку
    if not samples:
        raise ValueError("Итоговый список образцов пуст. Проверьте входные и исключаемые образцы, а также директорию с исходными файлами.")
    print(f'Найдено {found_samples}, из них будут обрабатываться {len_samples}.')
    # Возвращаем полный путь к каждому файлу
    return samples


def get_samples_in_dir(dir:str, extensions:tuple):
    """
    Генерирует список файлов на основе включающих и исключающих образцов.
    Выдаёт ошибку, если итоговый список пустой.

    :param dir: Директория, где искать файлы.
    :param extensions: Расширения файлов для поиска.
    :return: Список путей к файлам.
    """
    # Ищем все файлы в директории с указанными расширениями
    files = [os.path.join(dir, s) for s in os.listdir(dir) if s.endswith(extensions)]
    return files


def get_samples_in_dir_tree(dir:str, extensions:tuple):
    """
    Генерирует список файлов, проходя по дереву папок, корнем которого является dir.
    Выдаёт ошибку, если итоговый список пустой.

    :param dir: Директория, где искать файлы.
    :param extensions: Кортеж расширений файлов для поиска.
    :return: Список файлов с путями.
    """
    files = []
    for root, _ds, fs in os.walk(dir):
        samples = [os.path.join(root, f) for f in fs 
                    if f.endswith(extensions)]
        files.extend(samples)
    return files


def generate_sample_filenames(sample: str, folders: dict, filenames: dict) -> dict:
    """
    Генерирует словарь с путями к файлам для сэмпла на основе инструкций в filenames.

    :param sample: Имя сэмпла (строка).
    :param folders: Словарь с путями к директориям.
    :param filenames: Словарь с инструкциями для генерации файловых путей.
    :return: Словарь с результатами выполнения инструкций для файловых путей.
    """
    # Словарь для хранения сгенерированных путей
    generated_filenames = {}
    # Объединяем все переменные в один словарь для подстановки в eval()
    context = {
            'folders': folders,
            'sample': sample,
            'filenames': generated_filenames,
            'os': os  # Добавляем os в контекст, чтобы os.path был доступен
            }
    # Проходим по каждому ключу в filenames и вычисляем значение
    for key, instruction in filenames.items():
        # Используем eval() для вычисления выражений в строках
        try:
            # Заменяем {{ и }} на { и }
            instruction = instruction.replace('{{', '{').replace('}}', '}')
            # Выполняем eval с корректной строкой
            # Выполняем инструкцию, подставляя доступные переменные
            context['filenames'][key] = eval(instruction, context)
        except Exception as e:
            print(f"Ошибка при обработке {key}: {e}")
    
    return generated_filenames


def generate_commands(context:dict,
                      commands:dict, cmd_list:list):
    """
    Генерирует словарь с командами для сэмпла на основе инструкций в cmds_template.

    :param context: Словарь с со словарями, содержащими подстроки.
    :param commands: Словарь с инструкциями для создания команд.
    :return: Словарь с результатами выполнения инструкций для команд.
    """
    instruction:str

    # Словарь для хранения сгенерированных путей
    generated_cmds = {}

    # Проходим по каждому ключу в filenames и вычисляем значение
    errors = 0

    #print(cmd_list)
    for key in cmd_list:
        cmd_instructions = commands[key]
        if type(cmd_instructions) == list:
            timeout = cmd_instructions[0]
            instruction = cmd_instructions[1]
        else:
            timeout = 0
            instruction = cmd_instructions

        # Используем eval() для вычисления выражений в строках
        try:
            # Если команда требует выполнения как Python-код (например, f-строки), используем eval(), подставляя доступные переменные
            if instruction.startswith(("f'", 'f"')):
                generated_cmds[key] = [eval(instruction, context), timeout]
            else:
                # Если это обычная строка, просто сохраняем её без eval
                generated_cmds[key] = [instruction, timeout]
        except Exception as e:
            print(f"Ошибка при обработке {key}: {e}")
            print(instruction)
            errors += 1
    if errors > 0:
        exit(code=1)
    return generated_cmds


def create_paths(paths: list):
    """
    Принимает список путей и пытается их создать.
    Если путь создать невозможно, выводит ошибку и завершает программу.
    
    :param paths: Список путей для создания.
    """
    for path in paths:
        try:
            os.makedirs(path, exist_ok=True)
        except OSError as e:
            print(f"Ошибка при создании пути: {path}. Ошибка: {e}")
            raise SystemExit(f"Невозможно создать путь: {path}")


def run_cmds(cmds:dict, debug:str, timeout_behavior:str) -> tuple:
    RED = "\033[31m"
    GREEN = "\033[32m"
    WHITE ="\033[37m"
    
    unit_result = {'log':{},
                    'stdout':{},
                    'stderr':{}}
    exit_codes = {}
    status = True
    interruption = False
    for title, cmd_opts in cmds.items():
        cmd = cmd_opts[0]
        timeout = cmd_opts[1]

        print(f'\t\t\t{title}:', end='')

        # Выполнение команды
        run_result = run_command(cmd=cmd, timeout=timeout, debug=debug)

        # Сохранение результатов
        unit_result['log'][title] = run_result['log']
        unit_result['stdout'][title] = run_result['stdout']
        unit_result['stderr'][title] = run_result['stderr']
        
        # Логгирование результатов выполнения
        r = unit_result['log'][title]

        # Проверка успешности выполнения команды
        if r['status'] == 'FAIL':
            print(f' {RED}FAIL{WHITE}, exit code: {r["exit_code"]}. ', end='')
            status = False
        else:
            print(f' {GREEN}OK{WHITE}. ', end='')
        exit_codes.update({title:r["exit_code"]})
        print(f'Duration: {r["duration"]}.')
        for exit_code in exit_codes.values():
            if exit_code == 'INTERRUPTED':
                interruption = True
                return (unit_result, exit_codes, status, interruption)
            if exit_code == 'TIMEOUT':
                if timeout_behavior == 'next':
                    continue
                return (unit_result, exit_codes, status, interruption)
    return (unit_result, exit_codes, status, interruption)


def gather_logs(all_logs:dict, log_space:dict, log:dict, stdout:dict, stderr:dict, unit:str, unit_result:str) -> tuple:
    # Обновляем логи для текущего образца
    log.update({unit:unit_result['log']})
    stdout.update({unit:unit_result['stdout']})
    stderr.update({unit:unit_result['stderr']})
    # Сохраняем обновлённые данные в YAML
    update_yaml(file_path=log_space['log_data'], new_data=all_logs['log'])
    update_yaml(file_path=log_space['stdout_log'], new_data=all_logs['stdout'])
    update_yaml(file_path=log_space['stderr_log'], new_data=all_logs['stderr'])

    return (log, stdout, stderr)


def run_command(cmd:str, timeout:int, debug:str) -> dict:
    if timeout == 0:
        timeout=None
    # Время начала (общее)
    start_time = time.time()
    cpu_start_time = time.process_time()
    start_datetime = datetime.now().strftime("%d.%m.%Y %H:%M:%S")

    stdout, stderr = "", ""

    result = subprocess.Popen(args=cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                universal_newlines=True, executable="/bin/bash", bufsize=1, cwd=None, env=None)
    

    try:       
        # Ожидаем завершения с таймаутом
        stdout, stderr = result.communicate(timeout=timeout)
        # Построчно читаем стандартный вывод и ошибки в зависимости от уровня дебага
        if debug:
            streams = []
            if debug in ['errors', 'all']:
                streams.append(('STDERR', stderr.splitlines()))
            if debug in ['info', 'all']:
                streams.append(('STDOUT', stdout.splitlines()))

            for label, stream in streams:
                for line in stream:
                    print(f"{label}: {line.strip()}")

        duration_sec, duration, cpu_duration, end_datetime = get_duration(start_time=start_time, cpu_start_time=cpu_start_time)

        # Лог успешного выполнения
        return {
            'log': {
                'status': 'OK' if result.returncode == 0 else 'FAIL',
                'start_time': start_datetime,
                'end_time': end_datetime,
                'duration': duration,
                'duration_sec': duration_sec,
                'cpu_duration_sec': round(cpu_duration, 2),
                'exit_code': result.returncode
            },
            'stderr': stderr.strip() if stderr else '',
            'stdout': stdout.strip() if stdout else ''
        }

    except subprocess.TimeoutExpired:
        result.kill()
        stdout, stderr = result.communicate()
        duration_sec, duration, cpu_duration, end_datetime = get_duration(start_time=start_time, cpu_start_time=cpu_start_time)
        # Лог при тайм-ауте
        return {
            'log': {
                'status': 'FAIL',
                'start_time': start_datetime,
                'end_time': end_datetime,
                'duration': duration,
                'duration_sec': duration_sec,
                'cpu_duration_sec': round(cpu_duration, 2),
                'exit_code': "TIMEOUT"
            },
            'stderr': stderr.strip() if stderr else '',
            'stdout': stdout.strip() if stdout else ''
        }
    except KeyboardInterrupt:
        result.kill()
        print('INTERRUPTED')
        duration_sec, duration, cpu_duration, end_datetime = get_duration(start_time=start_time, cpu_start_time=cpu_start_time)
        return {
            'log':
                {'status': 'FAIL',
                'start_time':start_datetime,
                'end_time':end_datetime,
                'duration': duration,
                'duration_sec': duration_sec,
                'cpu_duration_sec': round(cpu_duration, 2),
                'exit_code': 'INTERRUPTED'},
            'stderr': stderr.strip() if stderr else '',
            'stdout': stdout.strip() if stdout else ''   
                }
    

def get_duration(duration_sec:float=0, start_time:int=0, cpu_start_time:int=0, precision:str='s') -> tuple:
    # Время завершения (общее)
    duration_sec = int(time.time() - start_time)
    
    # Форматируем секунды в дни, часы и минуты
    time_str = convert_secs_to_dhms(secs=duration_sec, precision=precision)

    cpu_duration = time.process_time() - cpu_start_time
    end_datetime = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    return (duration_sec, time_str, cpu_duration, end_datetime)


def convert_secs_to_dhms(secs:int, precision:str='s') -> str:
    # Форматируем секунды в дни, часы и минуты
    if precision not in ['d', 'h', 'm', 's']:
        raise ValueError("Неправильное указание уровня точности!")
    d, not_d = divmod(secs, 86400) # Возвращает кортеж из целого частного и остатка деления первого числа на второе
    h, not_h = divmod(not_d, 3600)
    m, s = divmod(not_h, 60)
    measures = {'d':d, 'h':h, 'm':m, 's':s}
    to_string = []
    # Разряд времени пойдет в результат, если его значение не 0
    for measure, val in measures.items():
        if val !=0:
            to_string.append(f'{val}{measure}')
        if measure == precision:
            break
    # Формируем строку и определяем уровни точности
    time_str = " ".join(to_string)
    if len(time_str) == 0:
        time_str = (f'< 1{precision}')
    return time_str