# Выгрузка в csv-файл данных SQL-SELECT-запросов напрямую из HANA
1) Установить на рабочей станции HANA Client
2) Проверить наличие установленной утилиты командной строки hdbsql.exe
3) подготовить командный файл cmdfile_name. Это текстовый файл с любым расширением, содержащий команды для БД

Пример <cmdfile_name> :
``SELECT * FROM "SAPBWP"."/BI0/TPLANT" WHERE objvers = 'A';``

4) Подключиться к HANA db из командной строки и выполнить командный файл:
```
hdbsql -n <host> -i <instance> -u <database_user> -p <database_user_password> -d <database_name> -o <outputfile_name> -F ; -I <cmd_file_name> -quiet -a -x -g ? -p -q <outputfile_name>
```

будет содержать результат выполнения команд из командного файла, без заголовков (благодаря опции -a) с разделителем ; (благодаря
опции -F) и без welcome-banner (благодаря опции -quiet) и информации о кол-ве выгруженных строк (благодаря опции -x). Вместо null-значений будет
выгружаться знак ? (благодаря опции -g).

Дополнительно, нужно подавить символ начала строки (благодаря опции -p) и конца строки (опция -q). Скрипт в cmdfile_name можно параметризировать
через переменные, которые передавать через опцию -V. -C убирает " вокруг строк (важно для генерации SQL запросов)
[Подробнее про командные опции](https://help.sap.com/docs/SAP_HANA_PLATFORM/6b94445c94ae495c83a19646e7c3fd56/c24d054bbb571014b253ac5d6943b5bd.html?locale=en-US&version=2.0.03)

Дополнительно:
* Пароль можно не передавать открытым текстом, если настроено SSO с HANA DB или Secure User Store.
* Можно формировать командные файлы для запуска с уровня ОС. И запускать их, например, из цепочек процессов или с рабочих станций пользователей.

Пример файла запуска hdbsql для Windows для выполнения на BHD
```
@echo off
"C:\Program Files\SAP\hdbclient\hdbsql.exe" -n sapbhpm.office.lenta.com -i 20 -u <USERNAME> -p <*****> -d BHD -I "C:
\Users\<USER>\Documents\Work\hdbsql\script01.sql" -a -x -o "c:\Temp\output01.txt"
pause
```

Пример командного файла (script01.sql)
```sql
SELECT TOP 6 HOST,PORT,SCHEMA_NAME,TABLE_NAME,PART_ID,MEMORY_SIZE_IN_TOTAL
 FROM m_cs_tables WHERE schema_name = 'SAPBWD';
```

Результат (output01.txt)
```
"sapbhpm",32003,"SAPBWD","/1BCAMDP/0BW:DAP:APRIORI_1~LCONTROLTAB",0,60860
"sapbhpm",32003,"SAPBWD","/1BCAMDP/0BW:DAP:APRIORI_1~LPMMLTAB",0,48204
"sapbhpm",32003,"SAPBWD","/1BCAMDP/0BW:DAP:APRIORI_2~LCONTROLTAB",0,60860
"sapbhpm",32003,"SAPBWD","/1BCAMDP/0BW:DAP:APRIORI_2~LPMMLTAB",0,48204
"sapbhpm",32003,"SAPBWD","/1BS/TEST_TABLE",0,42008
"sapbhpm",32003,"SAPBWD","/1BS/TEST_T_FK",0,29352
```
Чтобы пара логин-пароль отсутствовала в файле запуска, можно создать на хосте, откуда выполняется запуск, т.н. secure user store и использовать в тексте
скрипта ключ (KEY) вместо пары USER/PASSWORD

```
@echo off
"C:\Program Files\SAP\hdbclient\hdbsql.exe" -n hana_host -i 20 -U <KEY> -d <HANA_Tenant> -I "C:\Users\<...>\Documents\script01.sql" -a -x -o "c:\Temp\output01.txt"
pause
```

Для создания ключа выполняем
``"C:\Program Files\SAP\hdbclient\hdbuserstore" SET <KEY> "sapbhpm.office.lenta.com:32013@BHD" <USER> <PASSWORD>``

Для просмотра параметров подключения для данного KEY (без демонстрации пароля) выполняем
``"C:\Program Files\SAP\hdbclient\hdbuserstore" LIST <KEY>``