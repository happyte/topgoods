# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from mysql.connector import connect

class TopgoodsPipeline(object):
    def process_item(self, item, spider):
    	DBKWARGS = spider.settings.get('DBKWARGS')
    	con = connect(**DBKWARGS)
    	cur = con.cursor()
    	# cur.execute('create table genral_tab(good_price text,good_name text,good_url text,shop_name text,shop_url text,com_name text,com_add text)')
    	lis = (item['GOODS_PRICE'],item['GOODS_NAME'],item['GOODS_URL'],item['SHOP_NAME'],item['SHOP_URL'],item['COMPANY_NAME'],item['COMPANY_ADDRESS'])
    	try:
    	    cur.execute('insert into genral_tab(good_price,good_name,good_url,shop_name,shop_url,com_name,com_add) values (%s,%s,%s,%s,%s,%s,%s)',lis)
    	except Exception as e:
    	    print('error:',e)
    	    con.rollback()
    	else:
    	    con.commit()
    	    print('='*50)
    	con.close()
    	return item
