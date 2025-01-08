import os
import pandas as pd
import sweetviz as sv
import logging
from abc import ABC, abstractmethod
import json

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Handler(ABC):
    """
    Абстрактный базовый класс для всех обработчиков.
    """
    def __init__(self, next_handler=None):
        self.next_handler = next_handler

    def set_next(self, handler):
        """Устанавливает следующий обработчик в цепочке."""
        self.next_handler = handler
        return handler

    @abstractmethod
    def handle(self, data):
        """Обрабатывает данные и передает их следующему обработчику."""
        if self.next_handler:
            return self.next_handler.handle(data)
        return data


class OpenCsvHandler(Handler):
    """
    Шаг открытия CSV файла.
    """
    def __init__(self, file_path, file_name):
        super().__init__()
        self.file_path = file_path
        self.file_name = file_name

    def handle(self, data):
        # Полный путь к файлу
        dataset_path = os.path.join(self.file_path, self.file_name)

        # Проверка существования файла
        if not os.path.exists(dataset_path):
            raise FileNotFoundError(f"Файл {self.file_name} не найден по пути {self.file_path}.")

        logging.info(f"Чтение данных из файла {self.file_name}...")
        data["df"] = pd.read_csv(dataset_path)
        logging.info("Данные успешно загружены.")
        return super().handle(data)


class CleanDataHandler(Handler):
    """
    Шаг очистки и обработки данных.
    """
    def handle(self, data):
        df = data.get("df")
        if df is None:
            raise ValueError("Данные отсутствуют для обработки.")

        logging.info("Удаление дублей...")
        df.drop_duplicates(inplace=True)

        logging.info("Обработка пропусков...")
        for col in df.columns:
            if df[col].isnull().any():
                if df[col].dtype == 'object':
                    df[col].fillna(df[col].mode()[0], inplace=True)
                else:
                    df[col].fillna(df[col].median(), inplace=True)

        data["df"] = df
        logging.info("Данные очищены.")
        return super().handle(data)

class SweetvizHandler(Handler):
    """
    Шаг генерации отчета Sweetviz.
    """
    def __init__(self, output_dir):
        super().__init__()
        self.output_dir = output_dir
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def handle(self, data):
        df = data.get("df")
        if df is None:
            raise ValueError("Нет данных для создания отчета Sweetviz.")

        logging.info("Генерация отчета Sweetviz...")
        report_path = os.path.join(self.output_dir, "sweetviz_report.html")
        report = sv.analyze(df)
        report.show_html(filepath=report_path)
        logging.info(f"Отчет Sweetviz сохранен: {report_path}")
        return super().handle(data)


class EDAFlow:
    """
    Управление цепочкой обязанностей.
    """
    def __init__(self, config):
        self.config = config
        self.chain = None

    def build_chain(self):
        """
        Создает цепочку обязанностей.
        """
        self.chain = OpenCsvHandler(
            file_path=self.config["datasets_dir"],
            file_name=self.config["file_name"]
        )
        self.chain.set_next(CleanDataHandler()) \
                  .set_next(SweetvizHandler(output_dir=self.config["output_dir"]))

    def execute(self):
        """
        Запуск цепочки обязанностей.
        """
        if self.chain is None:
            raise RuntimeError("Цепочка не была построена.")
        logging.info("Начало выполнения цепочки анализа данных.")
        self.chain.handle(data={})

# Для запуска в colab необходимо заменить main и передавать путь к JSON файлу конфигурации в переменной или используя ввод из консоли

def main(config_path: str):
    # Загрузка конфигурации
    with open(config_path, 'r') as f:
        config = json.load(f)

    # Построение и выполнение EDAFlow
    eda_flow = EDAFlow(config)
    eda_flow.build_chain()
    eda_flow.execute()


if __name__ == "__main__":
    import argparse
    # Аргументы командной строки
    parser = argparse.ArgumentParser(description="EDA Flow с цепочкой обязанностей.")
    parser.add_argument("--config", type=str, required=True, help="Путь к JSON файлу конфигурации.")
    args = parser.parse_args()

    # Запуск основного процесса
    main(args.config)
