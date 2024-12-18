from datetime import datetime
import time
from src.utils import load_yaml, gather_logs, convert_secs_to_dhms, run_cmds


class CommandExecutor:
    def __init__(self, cmd_data:dict, log_space:dict, module:str, debug:str):
        """
        Инициализация CommandExecutor.
        
        :param cmd_data: Данные о командах.
        :param log_space: Лог-файлы для записи выполнения.
        :param module: Название модуля.
        """
        self.logs:dict
        self.debug:str

        self.debug = debug
        self.logs = {'log':load_yaml(file_path=log_space['log_data']),
                     'stdout':load_yaml(file_path=log_space['stdout_log']),
                     'stderr':load_yaml(file_path=log_space['stderr_log'])}
        self.cmd_data = cmd_data
        self.log_space = log_space
        self.module = module
        self.module_start_time = datetime.now().strftime("%d.%m.%Y_%H:%M:%S")

        # Инициализируем раздел логов для текущего модуля
        for log_type in ['log', 'stdout', 'stderr']:
            if f'{self.module}_{self.module_start_time}' not in self.logs[log_type]:
                self.logs[log_type][f'{self.module}_{self.module_start_time}'] = {}

    def execute(self, module_stages:list, module_result_dict:dict, timeout_behavior:str='') -> dict:
        """
        Выполняет команды для списка образцов.
        
        :param samples: Список образцов.
        :param module_result_dict: Данные о результатах выполнения пайплайна для каждого образца
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
                module_result_dict[module_stage] = {'status':True, 'programms':{}}
                # Получаем команды для стадии модуля
                cmds = self.cmd_data[module_stage]
                unit_result, exit_codes, status, interruption = run_cmds(cmds=cmds, debug=self.debug, timeout_behavior=timeout_behavior)
                if any(code != 0 for code in exit_codes.values()):
                        module_result_dict[module_stage]['status'] = False
                        module_result_dict['status'] = False
                module_result_dict[module_stage]['status'] = status
                module_result_dict[module_stage]['programms'].update(exit_codes)

                # Обновляем логи
                log_section, stdout_section, stderr_section = gather_logs(all_logs=self.logs, log_space=self.log_space,
                                                                          log=log_section, stdout=stdout_section, stderr=stderr_section,
                                                                          unit=module_stage, unit_result=unit_result)
                if interruption:
                    return module_result_dict
            else:
                # Счётчик отработанных образцов
                k = 0
                samples = self.cmd_data[module_stage].keys()
                for sample in samples:
                    module_result_dict[module_stage][sample] = {'status':True, 'programms':{}}
                    #s = module_result_dict['samples'][sample]
                    print(f'\t\tSample: {YELLOW}{sample}{WHITE}')

                    # Получаем команды для текущего образца
                    cmds = self.cmd_data[module_stage][sample]
                    unit_result, exit_codes, status, interruption = run_cmds(cmds=cmds, debug=self.debug, timeout_behavior=timeout_behavior)
                    if any(code != 0 for code in exit_codes.values()):
                        module_result_dict[module_stage][sample]['status'] = False
                        module_result_dict['status'] = False
                    module_result_dict[module_stage][sample]['programms'].update(exit_codes)
                    
                    # Обновляем логи
                    log_section, stdout_section, stderr_section = gather_logs(all_logs=self.logs, log_space=self.log_space,
                                                                            log=log_section, stdout=stdout_section, stderr=stderr_section,
                                                                            unit=module_stage, unit_result=unit_result)
                    
                    if interruption:
                        break
                                        
                    # Вывод статистики по времени, затраченному на обработку одного образца в рамках модуля
                    k+=1
                    avg_duration = (time.time()-start_time_module)/k
                    samples_remain = len(samples) - k
                    est_total_time = convert_secs_to_dhms(secs=int(avg_duration * samples_remain), precision='m')
                    print(f'{k}/{len(samples)}. Est. module completion time: {est_total_time} ')
                    
        return module_result_dict