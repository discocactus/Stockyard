#!/bin/bash -l

echo ' - - - - - - - - - - - - - - - -'
echo ''$(date +"%Y-%m-%d %a %H:%M:%S")' --- Start Cron'

echo ''$(date +"%Y-%m-%d %a %H:%M:%S")' --- Quit Screen [scrapy]'
screen -S scrapy -X quit
sleep 10

echo ''$(date +"%Y-%m-%d %a %H:%M:%S")' --- Start Screen [scrapy]
screen -d -m -S scrapy'
sleep 10

echo ''$(date +"%Y-%m-%d %a %H:%M:%S")' --- Clear Screen'
screen -S scrapy -X stuff 'clear^M'
sleep 2

echo ''$(date +"%Y-%m-%d %a %H:%M:%S")' --- Activate venv'
screen -S scrapy -X stuff 'source ~/python36/bin/activate^M'
sleep 5

echo ''$(date +"%Y-%m-%d %a %H:%M:%S")' --- Change Directory'
screen -S scrapy -X stuff 'cd scrapy_project^M'
sleep 2

echo ''$(date +"%Y-%m-%d %a %H:%M:%S")' --- Start [yahoo_stock_crawl]'
screen -S scrapy -X stuff 'scrapy crawl yahoo_stock_crawl^M'

