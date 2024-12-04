import duckdb


con = duckdb.connect("D:\\bing\\lab\\inner_stock\\stock\\data\\instockdb.duckdb")
con.sql('show tables').show()
con.sql('select * from cn_stock_selection').show()