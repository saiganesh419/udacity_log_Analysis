#!/usr/bin/env python

# to import postgress liabrary files
import psycopg2

# to import date module from datetime
from datetime import date


def output(que):
    try:
        db = psycopg2.connect("dbname = news")
        cu = db.cursor()
        cu.execute(que)
        res = cu.fetchall()
        db.close()
        return res
    except BaseException as error:
        print(error)

query1 = """select title, count(log.id) as views from articles, log
            where log.path = concat('/article/', articles.slug)
            group by articles.title order by views desc limit 3;"""
query2 = """select authors.name, count(*) as views from articles
           join authors on articles.author = authors.id join log
           on articles.slug = substring(log.path, 10)
           where log.status LIKE '200 OK'
           group by authors.name order by views DESC;"""
query3 = """select * from (
           select a.day,
           round(cast((100*b.hits) as numeric) / cast(a.hits as numeric), 2)
           as errp from (select date(time) as day, count(*) as hits
           from log group by day) as a inner join (select date(time) as day,
           count(*) as hits from log where status not like '200 OK'
           group by day)
           as b on a.day = b.day) as t where errp > 1.0 """


def popular_articles():
    pop = output(query1)
    print('What are the most popular three articles of all time?')
    print('')
    for jk in pop:
        print('"' + jk[0] + '" -- ' + str(jk[1]) + " views")
    print('')


def popular_authors():
    popular_author = output(query2)
    print('What are the most popular articles of all time?')
    print('')
    for m in popular_author:
        print('"' + m[0] + '" -- ' + str(m[1]) + ' views')
    print('')


def error_high_day():
    error_result = output(query3)
    print('On which days more than 1% of the requests led to error?')
    print('')
    for m, n in error_result:
        print "{:%B %d, %Y}".format(m), "--", n, '%', "errors"

    print('')


if __name__ == '__main__':
    print("Results:")
    print('')
    popular_articles()
    popular_authors()
    error_high_day()
