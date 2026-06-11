#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any


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


if __name__ == "__main__":
    def main() -> None:
        print("=== Code Nexus - Data Processor ===")

        print("Testing numeric Processor...")
        test1: list = [42, "Hello"]
        num = NumericProcessor()
        for example in test1:
            print(
                f" Trying to validate input '{example}':"
                f" {num.validate(example)}"
                )
        test2: str = "foo"
        print(
            "Test invalid ingestion of string"
            f" '{test2}' without prior validation:"
            )
        num.ingest(test2)
        test3: list[int] = [1, 2, 3, 4, 5]
        print(f"Processing data: {test3}")
        num.ingest(test3)
        num_to_extract: int = 3
        print(f"Extracting {num_to_extract} values...")
        for i in range(0, num_to_extract):
            key, value = num.output()
            print(f"numeric value {key}: {value}")

        print("\nTesting Text Processor...")
        txt = TextProcessor()
        print(
            " Trying to validate input "
            f"'{test1[0]}': {txt.validate(test1[0])}"
            )
        test4: list[str] = ['Hello', 'Nexus', 'World']
        print(f"Processing data: {test4}")
        txt.ingest(test4)
        num_to_extract = 1
        print(f"Extracting {num_to_extract} value...")
        for i in range(0, num_to_extract):
            key, value = txt.output()
            print(f"numeric value {key}: {value}")

        print("\nTesting log Processor...")
        log = LogProcessor()
        print(
            " Trying to validate input "
            f"'{test1[0]}': {log.validate(test1[0])}"
            )
        test5: list[dict] = [
            {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
            {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}
            ]
        print(f"Processing data: {test5}")
        log.ingest(test5)
        num_to_extract = 2
        print(f"Extracting {num_to_extract} value...")
        for i in range(0, num_to_extract):
            key, value = log.output()
            print(f"numeric value {key}: {value}")

    main()
