import requests
import os
import time
import threading
import json
import csv
import xlsxwriter

from argparse import ArgumentParser
from bs4 import BeautifulSoup
from collections import defaultdict
from typing import List
from datetime import datetime
from queue import Queue


HTML_DIR = "html_data"
OUTPUT_DIR = "output"
STOP_FLAG = "STOP"


def _write_file(filename: str, html: str):
    with open(filename, "w") as file:
        file.write(html)


def _read_file(file_path: str) -> str:
    with open(file_path) as file:
        src = file.read()
    return src


def check_exists_dir(dir: str):
    if not os.path.exists(dir):
        os.mkdir(path=dir)


def save_html(url: str, filename: str):
    time.sleep(0.5)
    req = requests.get(url)

    if req.status_code != 200:
        return save_html(url, filename)
    page = url.split("=")[-1]
    filename_html = filename + page + ".html"
    _write_file(os.path.join(HTML_DIR, filename_html), req.text)


def parse_html(filename_html: str) -> defaultdict:
    result = defaultdict(list)

    src = _read_file(os.path.join(HTML_DIR, filename_html))

    soup = BeautifulSoup(src, "lxml")
    dishes = soup.find_all("span", class_="emotion-1j2opmb")
    preparing_time = soup.find_all("span", class_="emotion-yelpk7")

    for num in range(len(dishes)):
        result[dishes[num].text] = preparing_time[num].text

    return result


class Format:
    """
    This is a parent class that is intended to be inherited by other classes
    """

    def __init__(self, filename: str, result: defaultdict, page: int):
        self.filename = filename
        self.result = result
        self.page = page

    def write2file(self):
        raise NotImplemented


class JSON(Format):
    """
    This is a subclass of the FORMAT class
    """
    fmt = ".json"

    def write2file(self):
        out_file = self.filename + self.fmt
        with open(os.path.join(OUTPUT_DIR, out_file), "a", encoding="utf-8") as file:
            json.dump(self.result, file, indent=4, ensure_ascii=False)


class CSV(Format):
    """
    This is a subclass of the Format class
    """
    fmt = ".csv"

    def write2file(self):
        out_file = self.filename + self.fmt
        items = [(key, value) for key, value in self.result.items()]

        with open(os.path.join(OUTPUT_DIR, out_file), "a", encoding="utf-8") as file:
            writer = csv.writer(file)
            if self.page == 1:
                writer.writerow(["dish", "preparing_time"])
            for num in range(len(self.result)):
                writer.writerow(items[num])


class XLSX(Format):
    """
    This is a subclass of the Format class
    """
    fmt = ".xlsx"

    def write2file(self):
        out_file = self.filename + self.fmt
        items = [(key, value) for key, value in self.result.items()]

        with xlsxwriter.Workbook(os.path.join(OUTPUT_DIR, out_file)) as workbook:
            worksheet = workbook.add_worksheet(f"page {self.page}")
            worksheet.write_row(0, 0, ["dish", "prepareing time"])
            for num in range(len(self.result)):
                worksheet.write_row(num+1, 0, items[num])


def check_fmt(result: defaultdict, filename: str, fmt: str, page: int) -> Format:
    if args.debug == True:
        assert fmt in {"json", "csv", "xlsx"}, f"Unknown file's format: {fmt}"

    fmt_dict = {
        "json": JSON(filename, result, page),
        "csv": CSV(filename, result, page),
        "xlsx": XLSX(filename, result, page)
    }

    return fmt_dict[fmt]


def write_meta(time_start: datetime, time_end: datetime, num_pages: int, num_notes: int):
    meta_data = {
        "time start": time_start.strftime("%H:%M:%S"),
        "time end": time_end.strftime("%H:%M:%S"),
        "duration": (time_end - time_start),
        "downloaded pages": num_pages,
        "downloaded notes": num_notes
    }
    fieldnames = [key for key, value in meta_data.items()]

    with open(os.path.join(OUTPUT_DIR, "meta.csv"), "w", encoding="utf-8") as meta_file:
        writer = csv.DictWriter(meta_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(meta_data)


def thread_add_html(queue_saver: Queue):
    page = 0
    while True:
        if page == args.num_pages:
            return queue_saver.put(STOP_FLAG)  # add break flag to end queue

        page += 1
        queue_saver.put(args.url + "?page=" + str(page))
        time.sleep(0.5)


def thread_save_html(queue_saver: Queue, queue_parser: Queue, filename: str):
    while True:
        time.sleep(0.5)
        url = queue_saver.get()
        if url == STOP_FLAG:
            return queue_parser.put(STOP_FLAG)

        req = requests.get(url)
        if req.status_code != 200:
            save_html(url, filename)
        page = url.split("=")[-1]
        filename_html = filename + page + ".html"
        _write_file(os.path.join(HTML_DIR, filename_html), req.text)
        queue_parser.put(filename_html)


def thread_parse_html(queue_parser: Queue, num_notes: List[int]) -> List[int]:
    time.sleep(0.5)
    while True:
        filename_html = queue_parser.get()
        if filename_html == STOP_FLAG:
            return num_notes

        if filename_html:
            result = parse_html(filename_html)
            # Save to file parsed data
            num_notes[0] += len(result)
            page = int(filename_html.split(".")[0][-1])
            input_fmt = check_fmt(result, args.filename, args.format, page)
            input_fmt.write2file()
        time.sleep(0.5)


def without_queue() -> int:
    num_notes = 0
    for page in range(1, args.num_pages+1):
        save_html(args.url + "?page=" + str(page), args.filename)
        result = parse_html(args.filename + str(page) + ".html")
        page_borders = [num_notes]
        num_notes += len(result)
        page_borders.append(num_notes)
        input_fmt = check_fmt(result, args.filename, args.format, page)
        input_fmt.write2file()

    return num_notes


def with_queue() -> int:
    num_notes = [0]
    queue_saver, queue_parser = Queue(), Queue()
    workers = []

    for num in range(args.num_put_workers):
        workers.append(threading.Thread(target=thread_add_html, args=(
            queue_saver,), name=f"Putter thread: {num+1}"))

    for num in range(args.num_save_workers):
        workers.append(threading.Thread(target=thread_save_html, args=(
            queue_saver, queue_parser, args.filename), name=f"Saver thread: {num+1}"))

    for num in range(args.num_parse_workers):
        workers.append(threading.Thread(
            target=thread_parse_html, args=(queue_parser, num_notes), name=f"Parser thread: {num+1}"))

    for tr in workers:
        tr.start()

    for tr in workers:
        tr.join()

    return num_notes[0]


if __name__ == '__main__':
    arg_parser = ArgumentParser()
    arg_parser.add_argument("--url", type=str, help="webpage url")
    arg_parser.add_argument("--filename", type=str,
                            help="name for html file without labels")
    arg_parser.add_argument("--num_pages", type=int, default=5,
                            help="number pages for parsing (maximum ~ 1000)")
    arg_parser.add_argument(
        "--format", type=str, default="json", help="choose format file: json/csv/xlsx")
    arg_parser.add_argument("--queue", type=bool,
                            default=False, help="turn on queue usage (True, False)")
    arg_parser.add_argument("--num_put_workers", type=int,
                            default=1, help="number threads for putting urls into queue")
    arg_parser.add_argument("--num_save_workers", type=int,
                            default=1, help="number threads for saving html files")
    arg_parser.add_argument("--num_parse_workers", type=int,
                            default=1, help="number threads for parsing html files")
    arg_parser.add_argument(
        "--debug", type=bool, default=False, help="debug mode for turn on asserts")

    args = arg_parser.parse_args()
    check_exists_dir(HTML_DIR)
    check_exists_dir(OUTPUT_DIR)

    time_start = datetime.utcnow()
    if not args.queue:
        num_notes = without_queue()

    elif args.queue:
        num_notes = with_queue()

    time_end = datetime.utcnow()
    write_meta(time_start, time_end, args.num_pages, num_notes)
