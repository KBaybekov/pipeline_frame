#!/usr/bin/env python3
from src import pipeline_manager, main_parser

def main():
    # Парсинг аргументов командной строки
    args = main_parser.parse_args()
    # Инициализация пайплайна
    pipeline = pipeline_manager.PipelineManager(args)
    #Запуск пайплайна
    pipeline.run_pipeline()

if __name__ == '__main__':
    main()