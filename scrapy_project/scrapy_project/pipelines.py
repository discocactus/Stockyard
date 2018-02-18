from scrapy.exceptions import DropItem

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
