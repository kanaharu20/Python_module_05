#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any
import typing


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data_stock: list[tuple[int, str]] = []
        self._processed_data_num: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    @abstractmethod
    def ingest(self, data: Any) -> None:
        ...

    def output(self) -> tuple[int, str]:
        return self._data_stock.pop(0)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        elif isinstance(data, list):
            for content in data:
                if not isinstance(content, (int, float)):
                    return False
            return True
        else:
            return False

    def ingest(self, data: Any) -> None:
        try:
            if not self.validate(data):
                raise TypeError("Got exception: Improper numeric data")
            if isinstance(data, list):
                for content in data:
                    self._data_stock.append(
                        (self._processed_data_num, str(content))
                        )
                    self._processed_data_num += 1
            else:
                self._data_stock.append((self._processed_data_num, str(data)))
                self._processed_data_num += 1
        except TypeError as e:
            print(e)


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (str)):
            return True
        elif isinstance(data, list):
            for content in data:
                if not isinstance(content, str):
                    return False
            return True
        else:
            return False

    def ingest(self, data: Any) -> None:
        try:
            if not self.validate(data):
                raise TypeError("Got exception: Improper text data")
            if isinstance(data, list):
                for content in data:
                    self._data_stock.append(
                        (self._processed_data_num, str(content))
                        )
                    self._processed_data_num += 1
            else:
                self._data_stock.append((self._processed_data_num, str(data)))
                self._processed_data_num += 1
        except TypeError as e:
            print(e)


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return True
        elif isinstance(data, list):
            for content in data:
                if not isinstance(content, dict):
                    return False
            return True
        else:
            return False

    def ingest(self, data: Any) -> None:
        try:
            if not self.validate(data):
                raise TypeError("Got exception: Improper log data")
            if isinstance(data, list):
                for content in data:
                    self._data_stock.append(
                        (self._processed_data_num, str(f"{content['log_level']}: {content['log_message']}"))
                        )
                    self._processed_data_num += 1
            else:
                self._data_stock.append((self._processed_data_num, str(f"{data['log_level']}: {data['log_message']}")))
                self._processed_data_num += 1
        except TypeError as e:
            print(e)


class DataStream():
    def __init__(self):
        self._processers: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processers.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        for data in stream:
            process_flag: int = 0
            for processer in self._processers:
                if processer.validate(data) is True:
                    processer.ingest(data)
                    process_flag = 1
                    break
            if process_flag == 0:
                print(
                    "DataStream error - "
                    f"Can't process element in stream: {data}"
                    )

    def print_processors_stats(self) -> None:
        if not self._processers:
            print("No processor found, no data\n")
        for processer in self._processers:
            print(
                f"{processer.__class__.__name__}: "
                f"total {processer._processed_data_num} "
                f"items processed, remaining {len(processer._data_stock)} "
                "on processor"
                )


if __name__ == "__main__":
    def main() -> None:
        numeric_proc: NumericProcessor = NumericProcessor()
        text_proc = TextProcessor()
        log_proc = LogProcessor()
        print("=== Code Nexus - Data Stream ===\n")
        print("Initialize Data Stream...")
        test_class = DataStream()
        print("== DataStream statistics ==")
        test_class.print_processors_stats()
        print("Registering Numeric Processor")
        test_class.register_processor(numeric_proc)
        test_data: list[
            str, list[float | dict | int],
            int, dict
            ] = [
                'Hello world', [3.14, -1, 2.71],
                [{'log_level': 'WARNING',
                    'log_message': 'Telnet access! Use ssh instead'},
                    {'log_level': 'INFO',
                     'log_message': 'User wil isconnected'
                     }], 42, ['Hi', 'five']]
        print(f"Send first batch of data on stream: {test_data}")
        test_class.process_stream(test_data)
        print("== DataStream statistics ==")
        test_class.print_processors_stats()
        print("Registering other data Processor")
        test_class.register_processor(text_proc)
        test_class.register_processor(log_proc)
        print("Send the same batch again")
        test_class.process_stream(test_data)
        print("== DataStream statistics ==")
        test_class.print_processors_stats()
        to_consume_num: int = 3
        to_consume_txt: int = 2
        to_consume_log: int = 1

        print(
            "\nConsume some elements from the data processors: Numeric "
            f"{to_consume_num}, Text {to_consume_txt}, Log {to_consume_log}"
            )
        for _ in range(to_consume_num):
            numeric_proc.output()
        for _ in range(to_consume_txt):
            text_proc.output()
        for _ in range(to_consume_log):
            log_proc.output()
        print("== DataStream statistics ==")
        test_class.print_processors_stats()


main()
