# Helper SELECTs
Для выгрузки в Hadoop надо соблюдать ряд требований к формату строк и данным в целом
* Строки, содержащие тексты, должны быть в двойных кавычках
* Если двойная кавычка - часть значения, то ей должен предшествовать \
* Строки, содержащие ключи, без кавычек, но есть ограничения на допустимые символы в строке
* NULL -значения, которые сохраняются как ?, должны быть заменены пустыми строками
* Выгрузка должна выполняться под DB-user, у которого LOCALE = 'ru'

Для подготовки SELECT для просмотра данных в Eclipse удобно использовать следующий SELECT (рассматривается в качестве примера задача выгрузки из HANA CV system-local.bw.bw2hana/ZHER_WGH4)
```sql
SELECT
 (case
 WHEN LOCATE(cname1, '___TXTMD"') > 0 THEN cname_rn
 WHEN LOCATE(cname1, '___TXTLG"') > 0 THEN cname_rn
 WHEN LOCATE(cname1, '___TXTSH"') > 0 THEN cname_rn
 WHEN LOCATE(cname1, '___T"') > 0 THEN cname_rn
 ELSE cname_n
 END) as cname
FROM (
SELECT '"' || column_name || '"' as cname1
 , 'ifnull("' || column_name || '",'''') as "' || column_name || '",' as cname_n
 , '''"'' || REPLACE(ifnull("' || column_name || '",''''),''"'',''\"'') || ''"'' as "' || column_name || '",' as cname_rn
 , position
 FROM view_columns
 WHERE view_name = 'system-local.bw.bw2hana/ZHER_WGH4')
 order by position;
```

Получив список полей, затем сконструировать из них SELECT и выполнить в Eclipse, проверяя данные. Пример SELECT ниже. В нем видно, что поля с только
ifnull - это строки с ключевыми значениями. А если в поле добавлены окаймляющие двойные кавычки и эскейп-символы, то это строки-тексты.
```sql
SELECT
ifnull("0PUR_GROUP",'') as "0PUR_GROUP",
'"' || REPLACE(ifnull("0PUR_GROUP___T",''),'"','\"') || '"' as "0PUR_GROUP___T",
'"' || REPLACE(ifnull("0PUR_GROUP___TXTMD",''),'"','\"') || '"' as "0PUR_GROUP___TXTMD",
ifnull("0RPA_WGH1",'') as "0RPA_WGH1",
'"' || REPLACE(ifnull("0RPA_WGH1___T",''),'"','\"') || '"' as "0RPA_WGH1___T",
'"' || REPLACE(ifnull("0RPA_WGH1___TXTMD",''),'"','\"') || '"' as "0RPA_WGH1___TXTMD",
ifnull("0RPA_WGH2",'') as "0RPA_WGH2",
'"' || REPLACE(ifnull("0RPA_WGH2___T",''),'"','\"') || '"' as "0RPA_WGH2___T",
'"' || REPLACE(ifnull("0RPA_WGH2___TXTLG",''),'"','\"') || '"' as "0RPA_WGH2___TXTLG",
ifnull("AGRSALDIR",'') as "AGRSALDIR",
'"' || REPLACE(ifnull("AGRSALDIR___T",''),'"','\"') || '"' as "AGRSALDIR___T",
'"' || REPLACE(ifnull("AGRSALDIR___TXTSH",''),'"','\"') || '"' as "AGRSALDIR___TXTSH",
ifnull("AGSALDIR2",'') as "AGSALDIR2",
'"' || REPLACE(ifnull("AGSALDIR2___T",''),'"','\"') || '"' as "AGSALDIR2___T",
'"' || REPLACE(ifnull("AGSALDIR2___TXTSH",''),'"','\"') || '"' as "AGSALDIR2___TXTSH",
ifnull("APUR_GRP",'') as "APUR_GRP",
'"' || REPLACE(ifnull("APUR_GRP___T",''),'"','\"') || '"' as "APUR_GRP___T",
'"' || REPLACE(ifnull("APUR_GRP___TXTMD",''),'"','\"') || '"' as "APUR_GRP___TXTMD",
ifnull("ZHER_WGH3",'') as "ZHER_WGH3",
'"' || REPLACE(ifnull("ZHER_WGH3___T",''),'"','\"') || '"' as "ZHER_WGH3___T",
'"' || REPLACE(ifnull("ZHER_WGH3___TXTLG",''),'"','\"') || '"' as "ZHER_WGH3___TXTLG",
'"' || REPLACE(ifnull("ZHER_WGH3___TXTMD",''),'"','\"') || '"' as "ZHER_WGH3___TXTMD",
ifnull("ZHER_WGH4",'') as "ZHER_WGH4",
'"' || REPLACE(ifnull("ZHER_WGH4___T",''),'"','\"') || '"' as "ZHER_WGH4___T",
'"' || REPLACE(ifnull("ZHER_WGH4___TXTLG",''),'"','\"') || '"' as "ZHER_WGH4___TXTLG",
'"' || REPLACE(ifnull("ZHER_WGH4___TXTMD",''),'"','\"') || '"' as "ZHER_WGH4___TXTMD",
ifnull("ZSALE_DIR",'') as "ZSALE_DIR",
'"' || REPLACE(ifnull("ZSALE_DIR___T",''),'"','\"') || '"' as "ZSALE_DIR___T",
'"' || REPLACE(ifnull("ZSALE_DIR___TXTMD",''),'"','\"') || '"' as "ZSALE_DIR___TXTMD"
FROM "_SYS_BIC"."system-local.bw.bw2hana/ZHER_WGH4" ORDER BY "ZHER_WGH4" ASC
```

Чтобы получившийся SELECT было удобнее вставлять в ABAP-код для формирования ABDC-запроса, рекомендуется собрать строки ABAP-кода следующим
```sql
SELECT
 SELECT
 '|' || (case
 WHEN LOCATE(cname1, '___TXTMD"') > 0 THEN cname_rn
 WHEN LOCATE(cname1, '___TXTLG"') > 0 THEN cname_rn
 WHEN LOCATE(cname1, '___TXTSH"') > 0 THEN cname_rn
 WHEN LOCATE(cname1, '___T"') > 0 THEN cname_rn
 ELSE cname_n
 END) || ' | & ' as cname
FROM (
SELECT '"' || column_name || '"' as cname1
 , 'ifnull("' || column_name || '",'''') as "' || column_name || '",' as cname_n
 , '''"'' \|\| REPLACE(ifnull("' || column_name || '",''''),''"'',''\\"'') \|\| ''"'' as "' || column_name || '",' as cname_rn
 , position
 FROM view_columns
 WHERE view_name = 'system-local.bw.bw2hana/ZHER_WGH4')
 order by position;
 ```

Получившийся код для ABAP. Обратите внимание на эскейп-символы \ для | и \, нужные уже для ABAP.

```sql
 lv_row-row = |SELECT ifnull("0PUR_GROUP",'') as "0PUR_GROUP", | &
 |'"' \|\| REPLACE(ifnull("0PUR_GROUP___T",''),'"','\\"') \|\| '"' as "0PUR_GROUP___T", | &
 |'"' \|\| REPLACE(ifnull("0PUR_GROUP___TXTMD",''),'"','\\"') \|\| '"' as "0PUR_GROUP___TXTMD", | &
 |ifnull("0RPA_WGH1",'') as "0RPA_WGH1", | &
 |'"' \|\| REPLACE(ifnull("0RPA_WGH1___T",''),'"','\\"') \|\| '"' as "0RPA_WGH1___T", | &
 |'"' \|\| REPLACE(ifnull("0RPA_WGH1___TXTMD",''),'"','\\"') \|\| '"' as "0RPA_WGH1___TXTMD", | &
 |ifnull("0RPA_WGH2",'') as "0RPA_WGH2", | &
 |'"' \|\| REPLACE(ifnull("0RPA_WGH2___T",''),'"','\\"') \|\| '"' as "0RPA_WGH2___T", | &
 |'"' \|\| REPLACE(ifnull("0RPA_WGH2___TXTLG",''),'"','\\"') \|\| '"' as "0RPA_WGH2___TXTLG", | &
 |ifnull("AGRSALDIR",'') as "AGRSALDIR", | &
 |'"' \|\| REPLACE(ifnull("AGRSALDIR___T",''),'"','\\"') \|\| '"' as "AGRSALDIR___T", | &
 |'"' \|\| REPLACE(ifnull("AGRSALDIR___TXTSH",''),'"','\\"') \|\| '"' as "AGRSALDIR___TXTSH", | &
 |ifnull("AGSALDIR2",'') as "AGSALDIR2", | &
 |'"' \|\| REPLACE(ifnull("AGSALDIR2___T",''),'"','\\"') \|\| '"' as "AGSALDIR2___T", | &
 |'"' \|\| REPLACE(ifnull("AGSALDIR2___TXTSH",''),'"','\\"') \|\| '"' as "AGSALDIR2___TXTSH", | &
 |ifnull("APUR_GRP",'') as "APUR_GRP", | &
 |'"' \|\| REPLACE(ifnull("APUR_GRP___T",''),'"','\\"') \|\| '"' as "APUR_GRP___T", | &
 |'"' \|\| REPLACE(ifnull("APUR_GRP___TXTMD",''),'"','\\"') \|\| '"' as "APUR_GRP___TXTMD", | &
 |ifnull("ZHER_WGH3",'') as "ZHER_WGH3", |.
 APPEND lv_row TO me->i_select_script.
 lv_row-row = |'"' \|\| REPLACE(ifnull("ZHER_WGH3___T",''),'"','\\"') \|\| '"' as "ZHER_WGH3___T", | &
 |'"' \|\| REPLACE(ifnull("ZHER_WGH3___TXTLG",''),'"','\\"') \|\| '"' as "ZHER_WGH3___TXTLG", | &
 |'"' \|\| REPLACE(ifnull("ZHER_WGH3___TXTMD",''),'"','\\"') \|\| '"' as "ZHER_WGH3___TXTMD", | &
 |ifnull("ZHER_WGH4",'') as "ZHER_WGH4", | &
 |'"' \|\| REPLACE(ifnull("ZHER_WGH4___T",''),'"','\\"') \|\| '"' as "ZHER_WGH4___T", | &
 |'"' \|\| REPLACE(ifnull("ZHER_WGH4___TXTLG",''),'"','\\"') \|\| '"' as "ZHER_WGH4___TXTLG", | &
 |'"' \|\| REPLACE(ifnull("ZHER_WGH4___TXTMD",''),'"','\\"') \|\| '"' as "ZHER_WGH4___TXTMD", | &
 |ifnull("ZSALE_DIR",'') as "ZSALE_DIR", | &
 |'"' \|\| REPLACE(ifnull("ZSALE_DIR___T",''),'"','\\"') \|\| '"' as "ZSALE_DIR___T", | &
 |'"' \|\| REPLACE(ifnull("ZSALE_DIR___TXTMD",''),'"','\\"') \|\| '"' as "ZSALE_DIR___TXTMD" | &
 | FROM "_SYS_BIC"."system-local.bw.bw2hana/ZHER_WGH4" ORDER BY "ZHER_WGH4" ASC;|.
 ```
 