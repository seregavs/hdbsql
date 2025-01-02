# hdbsql
ABAP framework to build and run SAP HANA data extraction to csv-files using hdbsql tool

# некоторые файлы
doc/zbw_hdbsql_csv.md - центральный файл документации по framework
src/zcl_bw_hdbsqlcsv_base.abap - код базового ABAP-класса
src/zcl_bw_hdbsqlcsv_g*s*.abap - примеры реализаций классов-потомков базового класса
src/zhdbsql_tbls.cds - определение служебных таблиц для framework
src/zbw_hdbsql_csv.abap - центральная ABAP-программа, которая выполняет заданную имплементацию класса для выгрузки данных

