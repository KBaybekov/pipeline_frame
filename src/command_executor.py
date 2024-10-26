from datetime import datetime
import time
from src.utils import load_yaml, gather_logs, get_duration, run_cmds


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

    def execute(self, module_stages:list, samples_result_dict:dict) -> dict:
        """
        Выполняет команды для списка образцов.
        
        :param samples: Список образцов.
        :param samples_result_dict: Данные о результатах выполнения пайплайна для каждого образца
        """
        cmds:dict
        # Цвета!
        YELLOW = "\033[33m"
        WHITE ="\033[37m"
        PURPLE = "\033[35m"
        
        # Получаем раздел логов для текущего модуля
        log_section = self.logs['log'][f'{self.module}_{self.module_start_time}']
        stdout_section = self.logs['stdout'][f'{self.module}_{self.module_start_time}']
        stderr_section = self.logs['stderr'][f'{self.module}_{self.module_start_time}']
        
        start_time_module = time.time()
        for module_stage in module_stages:
            print(f'\tStage: {PURPLE}{module_stage}{WHITE}')
            if module_stage != 'batch':
                samples_result_dict['samples'][module_stage] = {'status':True, 'programms':{}}
                # Получаем команды для стадии модуля
                cmds = self.cmd_data[module_stage]
                unit_result, exit_codes, status = run_cmds(cmds=cmds)
                samples_result_dict['samples'][module_stage]['status'] = status
                samples_result_dict['samples'][module_stage]['programms'].update(exit_codes)

                # Обновляем логи
                log_section, stdout_section, stderr_section = gather_logs(all_logs=self.logs, log_space=self.log,
                                                                          log=log_section, stdout=stdout_section, stderr=stderr_section,
                                                                          unit=module_stage, unit_result=unit_result)
            else:
                # Счётчик отработанных образцов
                k = 0
                samples = self.cmd_data[module_stage].keys()
                for sample in samples:
                    samples_result_dict['samples'][sample] = {'status':True, 'programms':{}}
                    #s = samples_result_dict['samples'][sample]
                    print(f'\t\tSample: {YELLOW}{sample}{WHITE}')

                    # Получаем команды для текущего образца
                    cmds = self.cmd_data[module_stage][sample]
                    unit_result, exit_codes, status = run_cmds(cmds=cmds)

                    # Обновляем логи
                    log_section, stdout_section, stderr_section = gather_logs(all_logs=self.logs, log_space=self.log,
                                                                            log=log_section, stdout=stdout_section, stderr=stderr_section,
                                                                            unit=module_stage, unit_result=unit_result)
                                        
                    # Вывод статистики по времени, затраченному на обработку одного образца в рамках модуля
                    k+=1
                    avg_duration = (time.time()-start_time_module)/k
                    samples_remain = len(samples) - k
                    est_total_time = get_duration(secs=int(avg_duration * samples_remain), precision='m')
                    print(f'{k}/{len(samples)}. Est. module completion time: {est_total_time} ')
                    
        return samples_result_dict