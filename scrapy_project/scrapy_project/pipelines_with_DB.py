from scrapy.exceptions import DropItem

from pymongo import MongoClient
import MySQLdb
import csv
import re
import pandas as pd


class ValidationPipeline(object):
    """
    Itemを検証するPipeline。
    """

    def process_item(self, item, spider):
        if not item['title']:
            # titleフィールドが取得できていない場合は破棄する。
            # DropItem()の引数は破棄する理由を表すメッセージ。
            raise DropItem('Missing title')

        return item  # titleフィールドが正しく取得できている場合。


class MongoPipeline(object):
    """
    ItemをMongoDBに保存するPipeline。
    """

    def open_spider(self, spider):
        """
        Spiderの開始時にMongoDBに接続する。
        """

        self.client = MongoClient('localhost', 27017)  # ホストとポートを指定してクライアントを作成。
        self.db = self.client['scraping-book']  # scraping-book データベースを取得。
        self.collection = self.db['items']  # items コレクションを取得。

    def close_spider(self, spider):
        """
        Spiderの終了時にMongoDBへの接続を切断する。
        """

        self.client.close()

    def process_item(self, item, spider):
        """
        Itemをコレクションに追加する。
        """

        # insert_one()の引数は書き換えられるので、コピーしたdictを渡す。
        self.collection.insert_one(dict(item))
        return item


class MySQLPipeline(object):
    """
    ItemをMySQLに保存するPipeline。
    """

    def open_spider(self, spider):
        """
        Spiderの開始時にMySQLサーバーに接続する。
        itemsテーブルが存在しない場合は作成する。
        """

        settings = spider.settings  # settings.pyから設定を読み込む。
        params = {
            'host': settings.get('MYSQL_HOST', 'localhost'),  # ホスト
            'db': settings.get('MYSQL_DATABASE', 'stockyard'),  # データベース名
            'user': settings.get('MYSQL_USER', ''),  # ユーザー名
            'passwd': settings.get('MYSQL_PASSWORD', ''),  # パスワード
            'charset': settings.get('MYSQL_CHARSET', 'utf8mb4'),  # 文字コード
        }
        self.conn = MySQLdb.connect(**params)  # MySQLサーバーに接続。
        self.c = self.conn.cursor()  # カーソルを取得。
        # itemsテーブルが存在しない場合は作成。
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS yahoo_fundamental (
                id INTEGER NOT NULL AUTO_INCREMENT,
                code integer,
                銘柄名 text,
                PER text,
                PBR text,
                EPS text,
                BPS text,
                PRIMARY KEY (id)
            )
        ''')
        self.conn.commit()  # 変更をコミット。

    def close_spider(self, spider):
        """
        Spiderの終了時にMySQLサーバーへの接続を切断する。
        """

        self.conn.close()

    def process_item(self, item, spider):
        """
        Itemをitemsテーブルに挿入する。
        """

        self.c.execute(
            'INSERT INTO yahoo_fundamental (code, 銘柄名, PER, PBR, EPS, BPS)'
            ' VALUES (%(code)s, %(銘柄名)s, %(PER)s, %(PBR)s, %(EPS)s, %(BPS)s)',
            dict(item))
        self.conn.commit()  # 変更をコミット。
        return item


class csvPipeline(object):
    """
    Itemをcsvに保存するPipeline。
    """
    def open_spider(self, spider):
        """
        Spiderの開始時に'yahoo_fundamental.csv'が存在しない場合は作成する。
        """

        try:
            with open('yahoo_fundamental.csv', 'x', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, ['date',
                                            'code',
                                            'name',
                                            'p_close',
                                            'open',
                                            'high',
                                            'low',
                                            'close',
                                            'volume',
                                            '売買代金',
                                            '値幅制限',
                                            '時価総額_百万円',
                                            '発行済株式数',
                                            '配当利回り',
                                            '配当',
                                            'per',
                                            'pbr',
                                            'eps',
                                            'bps',
                                            '最低購入代金',
                                            '単元株数',
                                            '年初来高値',
                                            '年初来安値',
                                            '信用買残',
                                            '信用買残前週比',
                                            '信用売残',
                                            '信用売残前週比',
                                            '貸借倍率',
                                            'get'
                                            ])
                writer.writeheader()
        except:
            print('\n\n --- File exists: yahoo_fundamental.csv ---\n\n')


    def close_spider(self, spider):
        """
        Spiderの終了時の動作。
        """
        df = pd.read_csv('yahoo_fundamental.csv')
        df = df.sort_values(['date', 'code', 'get'])
        df = df.drop_duplicates(['date', 'code'], keep='last').reset_index(drop=True)
        df.to_csv('yahoo_fundamental.csv', index=False)


    def process_item(self, item, spider):
        """
        Itemをファイルに挿入する。
        """
        with open('yahoo_fundamental.csv', 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, ['date',
                                        'code',
                                        'name',
                                        'p_close',
                                        'open',
                                        'high',
                                        'low',
                                        'close',
                                        'volume',
                                        '売買代金',
                                        '値幅制限',
                                        '時価総額_百万円',
                                        '発行済株式数',
                                        '配当利回り',
                                        '配当',
                                        'per',
                                        'pbr',
                                        'eps',
                                        'bps',
                                        '最低購入代金',
                                        '単元株数',
                                        '年初来高値',
                                        '年初来安値',
                                        '信用買残',
                                        '信用買残前週比',
                                        '信用売残',
                                        '信用売残前週比',
                                        '貸借倍率',
                                        'get'
                                        ])
            writer.writerows([dict(item)])

        return item
