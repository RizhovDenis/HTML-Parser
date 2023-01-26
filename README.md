# Descripsion
This script for parsing pages [website](https://eda.ru/recepty). You can choose format file for writing data: json, csv, xlsx. You can launch "queue" mode and set number workers.
* Download requirements
~~~
pip3 install -r requirements.txt
~~~
* Launch script
~~~
python3 parser.py --url 'https://eda.ru/recepty' --filename index --format csv --num_pages 100 --queue True
~~~
