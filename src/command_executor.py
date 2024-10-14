from datetime import datetime
from src.utils import run_command, load_yaml, update_yaml


class CommandExecutor:
    def __init__(self, cmd_data:dict, log_space:dict, module:str):
        """
        Инициализация CommandExecutor.
        
        :param cmd_data: Данные о командах.
        :param log_space: Лог-файлы для записи выполнения.
        :param module: Название модуля.
        """
        self.logs:dict

        self.logs = {'log':load_yaml(file_path=log_space['log_data']),
                     'stdout':load_yaml(file_path=log_space['stdout_log']),
                     'stderr':load_yaml(file_path=log_space['stderr_log'])}
        self.cmd_data = cmd_data
        self.log = log_space['log_data']
        self.stdout = log_space['stdout_log']
        self.stderr = log_space['stderr_log']
        self.module = module
        self.module_start_time = datetime.now().strftime("%d.%m.%Y_%H:%M:%S")

        # Инициализируем раздел логов для текущего модуля
        for log_type in ['log', 'stdout', 'stderr']:
            if f'{self.module}_{self.module_start_time}' not in self.logs[log_type]:
                self.logs[log_type][f'{self.module}_{self.module_start_time}'] = {}

    def execute(self, samples:list, samples_result_dict:dict) -> dict:
        """
        Выполняет команды для списка образцов.
        
        :param samples: Список образцов.
        :param samples_result_dict: Данные о результатах выполнения пайплайна для каждого образца
        """
        cmds:dict
        # Цвета!
        RED = "\033[31m"
        GREEN = "\033[32m"
        YELLOW = "\033[33m"
        WHITE ="\033[37m"
        
        # Получаем раздел логов для текущего модуля
        log_section = self.logs['log'][f'{self.module}_{self.module_start_time}']
        stdout_section = self.logs['stdout'][f'{self.module}_{self.module_start_time}']
        stderr_section = self.logs['stderr'][f'{self.module}_{self.module_start_time}']
        
        for sample in samples:
            samples_result_dict['samples'][sample] = {'status':True, 'programms':{}}
            s = samples_result_dict['samples'][sample]
            
            print(f'\tSample: {YELLOW}{sample}{WHITE}')

            # Получаем команды для текущего образца
            cmds = self.cmd_data[sample]

            sample_result = {'log':{},
                             'stdout':{},
                             'stderr':{}}

            for title, cmd in cmds.items():
                print(f'\t\t{title}:', end='')

                # Выполнение команды
                run_result = run_command(cmd=cmd)

                # Сохранение результатов
                sample_result['log'][title] = run_result['log']
                sample_result['stdout'][title] = run_result['stdout']
                sample_result['stderr'][title] = run_result['stderr']
                
                # Логгирование результатов выполнения
                r = sample_result['log'][title]

                # Проверка успешности выполнения команды
                if r['status'] == 'FAIL':
                    print(f' {RED}FAIL{WHITE}, exit code: {r["exit_code"]}. ', end='')
                    s['status'] = False
                else:
                    print(f' {GREEN}OK{WHITE}. ', end='')
                s['programms'].update({title:r["exit_code"]})

                print(f'Duration: {r["duration_sec"]} seconds.')
            
             # Обновляем логи для текущего образца
            log_section.update({sample:sample_result['log']})
            stdout_section.update({sample:sample_result['stdout']})
            stderr_section.update({sample:sample_result['stderr']})

            # Сохраняем обновлённые данные в YAML
            update_yaml(file_path=self.log, new_data=self.logs['log'])
            update_yaml(file_path=self.stdout, new_data=self.logs['stdout'])
            update_yaml(file_path=self.stderr, new_data=self.logs['stderr'])

        return samples_result_dict