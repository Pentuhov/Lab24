#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import duckdb
import typing as t
from pathlib import Path


def display_products(staff: t.List[t.Dict[str, t.Any]]) -> None:

    if staff:

        line = '+-{}-+-{}-+-{}-+-{}-+'.format(\
            '-' * 4,
            '-' * 30,
            '-' * 20,
            '-' * 15
        )
        print(line)
        print(
            '| {:^4} | {:^30} | {:^20} | {:^15} |'.format(
                "№",
                "Название.",
                "Товар",
                "Цена"
            )
        )
        print(line)


        for idx, product in enumerate(staff, 1):
            print(
                '| {:>4} | {:<30} | {:<20} | {:>15} |'.format(
                    idx,
                    product.get('name', ''),
                    product.get('shop', ''),
                    ", ".join(map(str,product.get('price', 0)))
                )
            )
        print(line)
    else:
        print("Список пуст.")


def create_db(database_path: Path) -> None:
    """
    Создать базу данных.
    """
    conn = duckdb.connect(str(database_path))
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE SEQUENCE IF NOT EXISTS shop_st START 1
        """
    )
    cursor.execute(
        """
        CREATE SEQUENCE IF NOT EXISTS product_st START 1
        """
    )

    # Создать таблицу с информацией о группах.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS shops (
            shop_id TEXT PRIMARY KEY ,
            shop_title TEXT NOT NULL
        )
        """
    )

    # Создать таблицу с информацией о студентах.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT NOT NULL,
            shop_id TEXT NOT NULL,
            product_price INT[] NOT NULL,
            FOREIGN KEY(shop_id) REFERENCES shops(shop_id)
        )
        """
    )

    conn.close()


def add_product(
        database_path: Path,
        name: str,
        shop: str,
        price: list):
    """
    Добавить данные о студенте
    """
    conn = duckdb.connect(str(database_path))
    cursor = conn.cursor()
    # Получить идентификатор группы в базе данных.
    # Если такой записи нет, то добавить информацию о новой группе.
    cursor.execute(
        """
        SELECT shop_id FROM shops WHERE shop_title = ?
        """,
        (shop,)
    )
    row = cursor.fetchone()
    if row is None:
        cursor.execute(
            """
            INSERT INTO shops VALUES (nextval('shop_st'),?)
            """,
            (shop,)
        )
        cursor.execute(
            """
            SELECT currval('shop_st')
            """
        )
        sel = cursor.fetchone()
        shop_id = sel[0]

    else:
        shop_id = row[0]

    # Добавить информацию о новом студенте.
    cursor.execute(
        """
        INSERT INTO products
        VALUES (nextval('product_st'),?, ?, ?)
        """,
        (name, shop_id, price)
    )

    conn.commit()
    conn.close()


def select_all(database_path: Path) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать всех студентов.
    """
    conn = duckdb.connect(str(database_path))
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT products.product_name, shops.shop_title, products.product_price
        FROM products
        INNER JOIN shops ON shops.shop_id = products.shop_id
        """
    )
    rows = cursor.fetchall()
    conn.close()
    return [
        {
            "name": row[0],
            "shop": row[1],
            "price": row[2],
        }
        for row in rows
    ]


def select_by_price(
        database_path: Path, shop: str
) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать студентов с заданной успеваемостью.
    """

    conn = duckdb.connect(str(database_path))
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT products.product_name, shops.shop_title, products.product_price
        FROM products
        INNER JOIN shops ON shops.shop_id = products.shop_id
        WHERE shops.shop_title = ?
        """,
        (shop,)
    )
    rows = cursor.fetchall()

    conn.close()
    return [
        {
            "name": row[0],
            "shop": row[1],
            "price": row[2],
        }
        for row in rows
    ]


def main(command_line=None):
    # Создать родительский парсер для определения имени файла.
    file_parser = argparse.ArgumentParser(add_help=False)
    file_parser.add_argument(
        "--db",
        action="store",
        required=False,
        default=str(Path.cwd() / "product.db"),
        help="The database file name"
    )

    # Создать основной парсер командной строки.
    parser = argparse.ArgumentParser("products")
    parser.add_argument(
        "--version",
        action="version",
        help="The main parser",
        version="%(prog)s 0.1.0"
    )

    subparsers = parser.add_subparsers(dest="command")

    # Создать субпарсер для добавления студента.
    add = subparsers.add_parser(
        "add",
        parents=[file_parser],
        help="Add a new product"
    )
    add.add_argument(
        "-n",
        "--name",
        action="store",
        required=True,
        help="The product's name"
    )
    add.add_argument(
        "-g",
        "--shop",
        action="store",
        help="The product's shop"
    )
    add.add_argument(
        "-gr",
        "--price",
        action="store",
        required=True,
        help="The product's price"
    )

    # Создать субпарсер для отображения всех студентов.
    _ = subparsers.add_parser(
        "display",
        parents=[file_parser],
        help="Display all products"
    )

    # Создать субпарсер для выбора студентов.
    select = subparsers.add_parser(
        "select",
        parents=[file_parser],
        help="Select the products"
    )
    select.add_argument(
        "-s",
        "--select",
        action="store",
        required=True,
        help="The required select"
    )

    # Выполнить разбор аргументов командной строки.
    args = parser.parse_args(command_line)

    # Получить путь к файлу базы данных.
    db_path = Path(args.db)
    create_db(db_path)

    # Добавить студента.

    if args.command == "add":
        add_product(db_path, args.name, args.shop, args.price)

    # Отобразить всех студентов.
    elif args.command == "display":
        display_products(select_all(db_path))

    # Выбрать требуемых студентов.
    elif args.command == "select":
        display_products(select_by_price(db_path, args.select))
        pass


if __name__ == '__main__':
    main()