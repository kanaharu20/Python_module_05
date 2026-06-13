#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any, Protocol
import typing


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...

class CSV():
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output")
        index, content = zip(*data)
        print(",".join(content))

class JSON():
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("JSON Output")
        index, content = zip(*data)
        i:int = 0
        print("{",end="")
        while index[i]:
            print(f'"item_{index[i]}": "{content[i]}"',end="")
            if i != len(index) - 1:
                print(",",end="")
            print("}")

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

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for processer in self._processers:
            data_to_output: list[tuple[int, str]] = []
            try:
                data_to_output.append(processer.output())
            except Exception:
                break
            plugin.process_output(data_to_output)



if 
