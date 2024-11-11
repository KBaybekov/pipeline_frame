from src.utils import generate_sample_list, generate_cmd_data, save_yaml, get_paths, create_paths
from src.pipeline_manager import PipelineManager
from src.command_executor import CommandExecutor
import os

class ModuleRunner:
    def __init__(self, pipeline_manager: PipelineManager):
        self.cmds_template:dict
        self.debug:list
        self.executables:dict
        self.exclude_samples:list
        self.include_samples:list
        self.folders: dict
        self.log_dir:str
        self.log_space:dict
        self.module_before: str
        self.modules: list
        self.modules_template: dict
        self.output_dir:str
        self.source_extensions: tuple
        self.subfolders:bool
        self.filenames: dict
        self.commands: dict
        self.cmd_data: dict
        self.__dict__= pipeline_manager.__dict__

    def run_module(self, module:str, module_result_dict:dict) -> dict:
        self.timeout_behavior=''
        self.proc_debug=''
        # Цвета!
        BLUE = "\033[34m"
        WHITE ="\033[37m"

        # Загружаем данные о модуле в пространство класса
        self.load_module(data=self.modules_template[module], input_dir=self.input_dir, output_dir=self.output_dir)

        # В зависимости от того, запущен модуль в одиночку либо перед ним отработал другой модуль, определяем папку входных данных
        if self.module_before in self.modules:
            self.input_dir = f'{self.output_dir}/{self.modules_template[self.module_before]["result_dir"]}'
            self.subfolders = False

        # Получаем список образцов
        self.samples = generate_sample_list(in_samples=self.include_samples, ex_samples=self.exclude_samples,
                                            input_dir=self.input_dir, extensions=self.source_extensions, subfolders=self.subfolders)
        # Генеририруем команды
        self.cmd_data = generate_cmd_data(args=self.__dict__, folders=self.folders,
                                    executables=self.executables, filenames=self.filenames,
                                    cmds_dict=self.commands, commands=self.cmds_template, samples=self.samples)
        # Логгируем сгенерированные команды для модуля
        save_yaml(f'cmd_data_{module}', self.log_dir, self.cmd_data)

        # Если режим дебага активен, возвращаем нужные данные и при необходимости завершаем выполнение
        if len(self.debug) !=0:
            debug_data = {'cmd_tpl': self.cmds_template,'samples': self.samples, 'cmds':self.cmd_data}
            for debug_item in self.debug:
                if debug_item == 'all':
                    print(debug_data)
                    self.proc_debug = 'all'
                if debug_item in debug_data.keys():
                    print(debug_item, debug_data[debug_item])
                if debug_item in ['errors', 'info']:
                    self.proc_debug = debug_item
            if 'demo' in self.debug:
                return module_result_dict

        # Алиас
        c = self.cmd_data

        # Создаём пути
        create_paths(list(self.folders.values()))
        # Инициализируем CommandExecutor
        exe = CommandExecutor(cmd_data=c, log_space=self.log_space, module=module, debug=self.proc_debug)

        # Выполняем команды для каждого образца
        print(f'Module: {BLUE}{module}{WHITE}')
        module_result_dict = exe.execute(c.keys(), module_result_dict, timeout_behavior=self.timeout_behavior)
        
        return module_result_dict
        

    def load_module(self, data:dict, input_dir:str, output_dir:str):
        """
        Загружает данные о модуле, обрабатывает их с использованием переменных в пространстве класса и\
                добавляет их в пространство объекта класса.
        """
        # Составляем полные пути для папок
        data['folders'] = get_paths(folders=data['folders'], input_dir=input_dir, output_dir=output_dir)
        # Устанавливаем атрибут modules_data в пространство экземпляра класса
        for key,value in data.items():
            if key == 'source_extensions':
                #Модифицируем список в кортеж для дальнейшего использования
                setattr(self, key, tuple(value))
                continue
            # Если ключ 'commands' и значение пустое, назначаем пустой список
            if key == 'commands':
                for group, val in data[key].items():
                    if val is None:
                        data[key][group] = []
            setattr(self, key, value)