# PROG ZBW_HDBSQL_CSV - Генерация скриптов для выгрузки данных BW в csv через hdbsql

Имя программы **ZBW_HDBSQL_CSV**.

## Введение
### Описание бизнес-процесса
Разработка необходима для автоматизации процесса подготовки скриптов, которые вызывают hdbsql для генерации csv-файлов с данными. Файлы
предназначены для передачи в Hadoop.
### Предыстория
Текущий подход в выгрузки данных из BWP-HANA в Hadoop предполагает обращение со стороны Hadoop к HANA EE, которая через SDA обращается к HANA
BWP и передает в Hadoop результаты выполнения SELECT-операторов. Для повышения производительности передачи иногда выполняют распараллеливание
SELECT-запросов: каждый запрос возвращает свой определенный диапазон данных. Неоднократно замечена высокая нагрузка на сервер HANA-BWP при
одновременном выполнении большого числа обращений со стороны HANA EE (и, в свою очередь, из Hadoop). Иногда Hadoop вызывал загрузку данных из
BWP, когда данные еще не были полностью рассчитаны. Это приводило к необходимости повторных выгрузок.
Для устранения недостатков текущего подхода был разработан и реализован новый подход: выгрузка csv-файлов данных из HANA BWP посредством утилиты
hdbsql (стандартная утилита SAP HANA).
### Достоинства нового подхода:
* более высокая производительность,
* упрощение ландшафта (HANA EE не требуется для этой задачи)
* бОльшая гибкость настройки правил формирования данных для выгрузки (SELECT-операторы)
* поддержка полной и дельта-выгрузки в csv
* управление регламентом выгрузки как со стороны источника данных (SAP BW), так и со стороны потребителя (Hadoop data engineers)
* ведения каталога выгрузок с указанием дельта-меток
* лицензионное соответствие SAP
### Недостатки
* необходим контроль занимаемого csv-файлами дискового пространства на files share
* большее кол-во шагов для настройки регулярной выгрузки

## Архитектура нового подхода "выгрузка hdbsql через csv"
В результате выполнения процессов выгрузки, на fileshare создается zip-файл, который содержит 2 файла
* csv-файл (разделитель полей - запятая, кодировка - UTF8) с данными; csv-файл генерируется SAP-утилитой hdbsql
* def-файл (имена полей и типов данных полей в csv); генерируется ABAP-программой ZBW_HDBSQL_CSV

Стандартная SAP HANA-утилита hdbsql запускается на стороне сервера приложений BW. "На вход" hdbsql подаются параметры командной строки и файл c
SELECT-выражением для выгрузки. В результате, hdbsql подключается к HANA BWP под пользователем BWREAD. (у пользователя BWREAD установлена Locale =
'ru' на уровне учетной записи). Параметры подключения сохранены на сервере приложений, в User Secure Store. После окончания выполнения hdbsql
генерирует csv-файл в кодировке UTF-8. Поскольку размеры файлов могут доходить до сотни гигабайт, для оптимизации их копирования на hdfs
предусмотрено их zip-архивирование с последующим (опциональным) удалением оригиналов.

Вся последовательность действий: вызов hdbsql, архивация zip, удаление csv выполняется из sh-скрипта, который генерируется данной ABAP-программой и
сохраняется в определенном каталоге на сервере приложений BW (операционная система AIX). Той же программой генерируется sql-файл с командой(ами)
SELECT для выгрузки. Ошибки выполнения hdbsql журналируется в errorlog_x.txt (x от 1 до 9), который перезатирается при каждом запуске и сохраняется в том
же каталоге, что и sh-скрипт. В случае отсутствия ошибок файл errorlog_ч.txt должен быть пустым. Иначе, текст каждой ошибки содержит ":". Именно этот
символ проверяется на последнем шаге в цепочке процессов (см ниже). Если такой символ в errorlog_x.txt есть, то шаг цепочки получит красный статус.

Команда SELECT генерируется в методе ABAP-класса. **Важно**: на каждую структуру данных выгрузки, а, точнее, на каждый {параллельный ] поток необходимо
создавать отдельный класс, наследуя его от ZCL_BW_HDBSQLCSV_BASE. В классе-потомке надо переопределить
* конструктор (обязательное требование для всех потомков)
* метод SET_SELECT_SCRIPT
* метод SET_SELECT_SCRIPT_TT
* метод GET_DELTA_TEXT
* (опционально) метод BEFORE_RUN_HDBSQL
* (опционально) метод UPDATE_MISSED
См. примеры классов ZCL_BW_HDBSQLCSV_G0004S001 (../src/zcl_bw_hdbsqlcsv_g0004s001.abap).

Класс с переопределенным методом BEFORE_RUN_HDBSQL - ZCL_BW_HDBSQLCSV_G0009S001 (../src/zcl_bw_hdbsqlcsv_g0009s001.abap).

Для запуска sh-скрипта используется цепочка процессов. Для каждой структуры данных выгрузки необходимо создать отдельную цепочку процессов, в
которой для каждого потока данных этой структуры последовательно вызываются следующие варианты процессов (в скобках - рекомендуемое техническое
имя шага, чтобы не запутаться при росте числа разных структур выгрузки, XXXX, XXX - номера групп и наборов соответственно):
1. ABAP-program: вызов ZBW_HDBSQL_CSV для формирования sh- и sql- файлов на сервере приложений BW. (ZGSXXXXSETXXX_MAKE) и def-файла
2. OS-command: вызов sh-файла. Если нет ошибок, то будет сформирован csv-файл данной структуры и для данного потока (ZGSXXXXSETXXX_SHHDBSQL)
3. (опционально) OS-command: анализ журнала выполнения. Если в журнале (errorlog_x.txt, x - от 1 до 9) встречается символ :, значит есть ошибка и
статус выполнения цепочки будет красным (ZGSXXXXSETXXX_CAT)

Пример цепочки - ZPC_GS002S002. Она настроена для одного потока одной структуры. В случае нескольких потоков одной структуры необходимо делать
отдельную цепочку для каждого потока или добавлять параллельные ветки в общую.
## Если надо ускорить выгрузку в csv
Можно применить распараллеливание внутри одного класса. Порядок настройки удобно иллюстрировать примером ZCL_BW_HDBSQLCSV_G0002S001
1. в таблице ZHDBSQLGS для G = g0002 и S = s001 определить несколько ( не более 10, номера POS от 0 до 9 включительно) позиций. В поле DELTA
указать критерий, который можно будет использовать в методе make_select_script для формирования текста SQL-запроса. И который (критерий)
позволит сконструировать запрос, который будет вызываться параллельно с другими запросами (не более 10).
2. В методе set_select_script получить DELTA из п.1 методом me->get_delta( ) и использовать полученное значение по усмотрению. В данном классе из
поля DELTA получают смещение в днях от текущей даты. Таким образом, в одном классе генерируется выполнение 10 параллельных SQL-SELECT-запросов, i от 0 до 9, каждый из которых собирает данные за интервал.

Например, [ текущая дата "минус" ( d "минус" 2 ), текущая дата "минус" ( d ) ], а значения d (поле ZHDBSQLGS-DELTA) из {1;4;7;10;13;16;...}

Важно обратить внимание, что если в настроечной таблице ZHDBSQLGS кол-во записей для одной G и S более 1, то sh-скрипт генерируется так, чтобы запускать несколько параллельных запросов на выполнение. Это делается добавлением & в конце вызова команды. См. метод MAKE_PRIMARY_SCRIPT, счетчик lv_count.

Если в данных критерий распараллеливания трудно найти, можно использовать первую букву в текстовом поле и "нарезать" по этому критерию на болееменее одинаковые по мощности множества для выгрузок. В "нарезке" для MCLIENT поможет такой запрос
MCLIENT, определение примерно равномощных множеств
```sql
SELECT substr(name2,1,1) as letter1, count(*) as cnt FROM "SAPBWP"."/BIC/PCLIENT"
 GROUP BY substr(name2,1,1) ORDER BY 2 DESC;
SELECT * FROM "SAPBWP"."/BIC/MCLIENT"
 WHERE substr(name2,1,1) IN ('','','');
```
## Генерация def-файла
Файл предназначен для автоматизации создания стрктуры таблицы в Hive в ходе импорта данных в hadoop.

Механизм генерации: специальный SQL-SELECT запрос (определяется методом SET_SELECT_SCRIPT_TT) используется при создании локальной временной таблицы (ЛВТ). Когда ЛВТ создается, то можно из HANA-словаря считать описание полей и их типов и сохранить описание в def-файле. По окончанию считывания ЛВТ автоматически удаляется. Также, временная таблица удаляется автоматически при отключении пользователя от HANA. Важно, чтобы текст SQL-SELECT-запроса для данной
задачи был таким, чтобы
* содержал идентичные поля (названия, типы) и в идентичном порядке, как и в основном SQL-SELECT, который возвращает данные для csv-файла
* не содержал много строк
* корректность типов (особенно, длин строк) можно гарантировать использованием в SQL-SELECT функций типа substring
## Структура каталогов для хранения файлов и правила наименования
File-share в Tcode:AL11 - DIR_HDBSQL. Корневой каталог для выгрузки - hdbsql. Имена и пути одинаковые на всех серверах (D/Q/P) ландшафта
Правила именования каталогов на файловой системе сервера приложений BW:
* Каждой структуре (или в терминах программы - группе (group)) соответствует отдельный каталог. Имя каталога - gXXXX, где XXXX - число. Каждая группа должна содержать как минимум один поток (см ниже)
* Каждому потоку (в терминах программы - набору (set)) соответствует отдельный подкаталог в каталоге группы. Имя подкаталога - sXXX, где XXX - число
* Внутри подкаталога с набором 2 подкаталога - arch (для хранения select-скриптов), zip (для хранения zip-архивов с csv-файлами).
* Файлы sh И csv сохраняются в корне sXXX.
* Для хранения каталогов с наборами, которые "устарели", но их по ряду причин надо сохранить, предназначен подкаталог arch каталога группы gXXXX.

Пример: sh-файл для группы 0001 потока 001 должен быть сохранен в /mnt/hdbsqlexp/hdbsql/g0001/s001
После создания системы каталогов для каждой новой группы/набора необходимо дать
* всем aix-пользователям сервера приложений и пользователю, под которым будет подключаться hadoop, право на запись в эти каталоги ``(chmod o+w dir_name)``.

Если каталоги создавались под aix-пользователем bwadm (как это и должно быть, т.к. эта задача выполняется SAP Basis), то шаг с присвоениями полномочий
bwadm можно опустить.
## Настройка delta-выгрузки
При необходимости, можно настроить дельта-выгрузку, при которой sh-скрипт будет генерировать csv-файлы, содержащие записи с большим значением т.н.
дельта-метки, чем при последней выгрузке. Дельта-сценарий возможен, если в выгружаемых данных есть поле, которое можно использоваться для дельтаметки. Обычно, это метка времени создания/изменения записи, но могут быть и другие поля. Какими бы они ни были, WHERE-ограничение с использованием
дельта-метки должно быть сгенерировано, а само значение метки на момент выгрузки - сохранено, чтобы при последующем извлечении получать данные с
большим (чем сохранено) значением этой метки.
Для сохранения меток используется таблица ZHDBSQLGS, поле DELTA (Char(250)). В этом поле допускается хранить любую информацию, достаточную для
корректной генерации SELECT-выражения в методе SET_SELECT_SCRIPT.
В случае необходимости поддержки дельта-обновления метод SET_SELECT_SCRIPT имеет следующий алгоритм работы:
1) получение метки времени последнего извлечения (чтение из таблицы ZHDBSQLGS), метод GET_DELTA
2) генерация SELECT-скрипта с добавлением условия WHERE типа "delta_field" > считанная_на_шаге_1_дельта-метка
3) получение последнего значения (на момент выполнения) дельта-метки
4) сохранение полученного значения дельта-метки в ZHDBSQLGS (метод SET_DELTA)
## Журналирование процесса выгрузок
Для облегчения контроля того, какие выгрузки, когда и с какими параметрами были выполнены, реализована (пока) 1-ая версия механизма журналирования
процесса выгрузки. Используется подход, заключающийся в сохранении в отдельной таблице ZHDBSQLLOG копии записей ZHDBSQLGS, релевантных данной
выгрузке, непосредственно после ее выполнения. В поле ZHDBSQLLOG-RUNTS указывается timestamp времени выгрузки. За сохранение отвечает zcl_bw_hdbsqlc
sv_base->make_log.
Первая версия журналирования предназначена для пилотной эксплуатации и не вызывает zcl_bw_hdbsqlcsv_base->make_log в конце каждой выгрузки.
## Генерация alert-файла
В отдельных случаях требуется высокая производительность передачи данных в Hadoop, которую текущий "hdbsql-движок" обеспечить не может. Дело в том,
что в текущей версии "hdbsql-движка" параллелизация достигается явным указанием в настройках числа параллельных процессов и настройках каждого из
них в отдельности. И их число не может превышать 10. В то же время, выгрузки из HANA EE в Hadoop могут работать в несколько десятков параллельных
процессов. Да, в это время нагрузка на сервер HANA высока, но если это приемлемо, но и выгрузка осуществляется многократно быстрее.
Пока движок не доработан, выгрузку данных для критичных по производительности процессов целесообразно оставить по текущей технологии, т.е. через
обращения из Hadoop к HANA EE за данными из виртуальных таблиц, которые "смотрят" на HANA CV в BWP.
Для своевременного информирования Hadoop о готовности данных в BW для выгрузки необходима программа, которая будет создавать alert-файл в той же
папке, в которой создавались бы zip-архивы c данными. Hadoop, "увидев" alert-файл в папке, начнет загрузку по текущей технологии, а по ее успешному
завершению - удалит alert-файл.
О том, какие изменения необходимо сделать в классе для выгрузки, см. в комментариях к ZCL_BW_HDBSQLCSV_BASE->SET_HDBSQL_SCRIPT
## Выгрузка строковых значений
Hadoop принимает строковые значения в двойных кавычках. Если кавычки являются частью строкового значения то знак кавычки должен предваряться \. В
строковом значении не должно быть переносов строки. Чтобы реализовать все эти требования в полном объеме, необходимо использовать следующую
конструкцию в выражении SELECT (на примере поля 0PLANT___TXTLG из запроса из класса ZCL_BW_HDBSQLCSV_G0020S001.

Так выглядит строка - часть запроса для HANA
```'"' || REPLACE_REGEXPR('[\r\n]' IN REPLACE_REGEXPR('(["\\])' IN ifnull("0PLANT___TXTLG",'') WITH '\\\1') WITH '') || '"' as "0PLANT___TXTLG".```

А так - строка - часть запроса для HANA, но создаваемая в ABAP-коде (нужны дополнительные escape-символы, и строка выглядит совсем уж громоздкой)
``` sql lv_row-row = | '"' \|\| REPLACE_REGEXPR('[\r\\n]' IN REPLACE_REGEXPR('(["\\\\])' IN ifnull("0PLANT___TXTLG",'') WITH '\\\\\\1') WITH '') \|\| '"' as "0PLANT___TXTLG", | & ```  

Проверять работу регулярных выражений в HANA. Например, в части переносов строк) удобно таким проверочным запросом:
```sql 
SELECT REPLACE_REGEXPR('[\r|\n]' IN ss1 WITH '') AS ss1_r
, REPLACE_REGEXPR('[\r|\n]' IN ss2 WITH '') AS ss2_r, ss1, ss2
FROM (
SELECT 'AAA' || char(10) || 'BBB' || char(10) || 'CCC' AS ss1
, 'AAA' || char(10) || char(13) || 'BBB' || char(10) || char(13) || 'CCC' AS ss2
FROM dummy);
```
В части замены символов на символы с эскейпированием.
```sql
SELECT REPLACE_REGEXPR('(["\\])' IN 'He!l\l\o, man"' WITH '\\\1') FROM dummy;
```
рефакторинг кода формирования текста SQL-SELECT
```sql
 lv_start_regex = | '"' \|\| REPLACE_REGEXPR('[\r\n]' IN REPLACE_REGEXPR('(["\\\\])' IN ifnull(|.
 lv_end_regex = |,'') WITH '\\\\\\1') WITH '') \|\| '"' as|.
 DATA(lv_delta) = me->get_delta( ).
 lv_row-row =
 | SELECT | &
 |"ACCNT_GRP" as "0ACCNT_GRP", | &
 |"ADDR_NUMBR" as "0ADDR_NUMBR", | &
 |"BIRTHDAY" as "0BIRTHDAY", | &
 |"BPARTNER" as "0BPARTNER", | &
 |{ lv_start_regex } "CITY" { lv_end_regex } "0CITY", | &
 |"COUNTRY" as "0COUNTRY", | &
 |"CUST_CLASS" as "0CUST_CLASS", | &
 |{ lv_start_regex } "EMAIL_ADDR" { lv_end_regex } "0EMAIL_ADDR", | &
 |"GENDER" as "0GENDER", | &
 |"INDUSTRY" as "0INDUSTRY", | &
 |"LANGU" as "0LANGU", | &
 |"LOGSYS" as "0LOGSYS", | &
 |{ lv_start_regex } "NAME" { lv_end_regex } "0NAME", | &
 |{ lv_start_regex } "NAME2" { lv_end_regex } "0NAME2", | &
 |{ lv_start_regex } "NAME3" { lv_end_regex } "0NAME3", | &
 |{ lv_start_regex } "PHONE" { lv_end_regex } "0PHONE", | &
 |{ lv_start_regex } "POSTAL_CD" { lv_end_regex } "0POSTAL_CD", | &
 |{ lv_start_regex } "REGION" { lv_end_regex } "0REGION", | &
 |{ lv_start_regex } "STREET" { lv_end_regex } "0STREET", | &
 |{ lv_start_regex } "TEL_NUMBER" { lv_end_regex } "0TEL_NUMBER", | &
 ```

Py-code для генерации abap-выражений по большим исходным sql-scripts (..\src\testsql4abap.py)
Если в hdbsql передается не SELECT-запрос, а anonymous SQL-script block вида DO BEGIN ... END;, то в зависимости от ОС, необходимо завершать этот блок поразному:
* для windows - после последней ; добавлять @
* для unix - оставлять как есть, без @
Если этот sql-script block с символом @ в конце выполнить на unix-хосте, то будет ошибка на этот символ.
Если этот sql-script block без символа @ в конце выполнить на windows-хосте, то будет ошибка на неправильный код.

## Порядок действий при настройке новой выгрузки
1. Разработать и отладить SELECT-оператор для выгрузки данных. При необходимости - продумать дельта-извлечение, разбиение на параллельные
потоки.
2. Создать на серверах приложений BWD/BWQ/BWP необходимую структуру каталогов (команды mkdir). Предоставить к ним необходимые доступы
(при необходимости) для aix-пользователей bwadm и hadoop-пользователя. (см. Bash-скрипты ниже, выполняет SAP-Basis)
3. Для каждого набора (потока) создать в таблице ZHDBSQLGS строку с заполненными полями G, S, POS и DESCRIPTION. Использовать Tcode SE16 или
(что удобнее) SM30 (ракурс ZHDBSQLGS )
4. Выполнить настройки на стороне Hadoop для:
   * чтения из каталога zip и распаковки zip-архивов в hdfs
   * загрузки csv-файлов заданной структуры в БД Hadoop.
5. Создать суб-класс от базового класса ZCL_BW_HDBSQLCSV_BASE. И переопределить
   * конструктор
   * метод SET_SELECT_SCRIPT (который отвечает за создание текста SELECT-запроса).
   * метод SET_SELECT_SCRIPT_TT (который отвечает за создание текста SELECT-запроса для временной таблицы)
   * метод GET_DELTA_TEXT (который отвечает за суффикс имения csv-файла)
   * (опционально) метод BEFORE_RUN_HDBSQL (который отвечает за действия, выполняемые непосредственно перед запуском выгрузки. Например, за формирование данных в таблице, из которой будет выполняться выгрузка)
   * (опционально) Если с Hadoop согласовано получение данных в формате gzip вместо zip, то выполнить модификацию конструктора класса,
как описано RFC-030090 Потребности в Hadoop. Организация выгрузки csv
6. При указании имен таблиц/ракурсов из BW-схемы не использовать имя схемы как константу: SAPBWD или SAPBWP. Вместо этого считывать имя
схемы из атрибута I_DBSCHEMA - он заполняется в конструкторе супер-класса.
7. Создать вариант запуска программы ZBW_HDBSQL_CSV для каждого нового набора новой группы. Рекомендуется придерживаться правила
именования вариантов ZGXXXXSXXX, где XXXX и XXX - номера группы и набора соответственно. Используйте как образец вариант ZG0004S001.
8. (опционально) Выполнить тестирование работы программы на BWD, на данных BWD. Программа должна генерировать технически корректные sh и
sql-файлы. Вы можете протестировать работу сгенерированных скриптов вызова hdbsql как со своего локального компьютера, так и на сервере
приложений BWD (необходим терминальный доступ через PuTTy).
9. (опционально) Выполнить тестирование работы bat-скрипта на данных BWP. Для этого в bat-скрипте поменять имя usersecstore на продуктивное и
заменить имя схемы на SAPBWP в файле *.sql. Тестирование выполнять на локальном компьютере, но необходимо учесть возможный большой
входящий траффик. По итогам тестирования оценить размеры csv-файлов и достаточность свободного места на file share.
10. (опционально) Выполнить пробную загрузку csv-файлов в Hadoop.
11. Создать цепочку (в группе ZHDBSQL) процесса для запуска. Использовать как образец цепочку ZPC_GS004S001. Настоятельно рекомендуется
следовать правилам именования вариантов процессов (см выше). Для редактирования доп. параметров в варианте "Команда ОС" нужны
полномочия на значение 01 операции ACTVT объекта S_RZL_ADM. Выдают по запросу (роль LO:S_RZL_ADM_01).
12. Согласовать регламент запуска цепочки. Возможно, встраивание цепочки в другие цепочки с уже установленным регламентом. Согласовать со
специалистами Hadoop получение ими zip-файлов c данными согласно регламентам. Предполагается, что в продуктивном режиме работы
 1. csv-файлы после zip-архивации будут удаляться, чтобы не занимать место на file share. Команда на удаление есть в sh-скрипте
 2. zip-архивы будут перемещаться (не копироваться!!) в hadoop hdfs. И это перемещение будет выполняться скриптами со стороны hadoop. **Важно** проконтролировать этот аспект, иначе папка с zip-архивами будет быстро расти в размерах
 3. sql-файлы с операторами SELECT сохраняются в каталоге arch. В случае выбора опции добавления time_stamp к имени файла, sql-файлы, по
умолчанию, не удаляются, и это позволяет посмотреть историю выгрузок. Добавление time_stamp определяется на селекционном экране
программы, поле pttimest.
13. Для выгрузок по требованию (в т.ч. силами специалистов Hadoop) им необходимо получить файлы sh и sql. Это можно сделать как сервера BWD/BWP,
так и выбрав режим генерации локальных скриптов в программе. В этом случае, скрипты будут сохранены в папку C:\Temp. При необходимости,
нужно скорректировать пути к файлам в sh-/bat-скриптах и user sec store, и удалить лишние команды в sh-скрипте
14. Перенести все настройки (запись в ZHDBSQLGS, вариант программы, класс(ы), цепочку) в BWP и запланировать выполнение. Нести по ландшафтам
только измененные актуальные записи.

Bash-скрипты для создания каталогов на серверах приложений BWD/Q/P. Перед запуском скорректируйте номера 0004, 0010 и 001,002 в соответствии с
вашими задачами. Данный скрипт адаптируется и передается в SAP Basis для выполнения.
```
mkdir g{0004..0010}
mkdir g{0004..0010}/s{001..002}
mkdir g{0004..0010}/s{001..002}/zip
mkdir g{0004..0010}/s{001..002}/arch
chmod -R 777 g{0004..0010) 
```

bash-скрипт для удаления файлов с расширениями csv/zip, старше заданного кол-ва дней
```
```


## Сценарий тестирования
Перед переносом выгрузки в продуктив рекомендуется выполнить функциональное тестирование в системе разработки. Это позволит сэкономить
существенное время, которое мы тратим на согласование и переносы объектов. Сценарий выполняется в системе разработки и состоит из нескольких шагов
1. Завершена разработка класса
2. Выполнено тестирование класса на данных BWD ( puserss = BWREAD ). Это тестирование покажет техническую корректность SQL-запросов в классе.
Настоятельно рекомендуется выполнять первое тестирование после завершение разработки именно через соединение BWREAD, т.е. на данных BWD
3. Переключить параметр puserss на BWREADPRD. Сделать это переключение в варианте программы ZBW_HDBSQL_CSV, которые вызываются из
цепочки процессов. Теперь программа, запущенная в BWD с данным вариантом, будет выгружать данные из BWP. Пока не запускать программу или
цепочку с ней.
4. Проверить текст генерируемого SQL-SELECT для данных. BWP содержит существенно больше данных, чем BWD и сохранение файла с продуктивными
данными на дисках сервера BWD может привести к переполнению квоты на каталоги для выгрузки hdbsql. Возможно, для целей тестирования вам
потребуется заменить SELECT на SELECT TOP 1000 или типа того. Сделать такую замену временно, в коде метода класса, до окончания тестирования в
BWD на данных BWP.
Также, установите переключатель pkeepcsv = X. Так вы будете сохранять csv-файл после выгрузки и сможете контролировать его визуально, в AL11.
5. Выполнить цепочку процессов или выполнить отдельно программу ZBW_HDBSQL_CSV с тестируемым вариантом, измененным на шаге 3. csv-Файл
генерируется sh-скриптом. А sh-script - генерируется программой ZBW_HDBSQL_CSV. Если все отработало без технических ошибок, проверьте
визуально получившийся файл в AL11. После успешной проверки сообщите команде дата-инженеров о том, что они могут загрузить файл с
продуктивными данными из папки на сервере разработки
6. После получения ОК от дата-инженеров, что такая загрузка выполнена без ошибок, подготовьте настройки для переноса в BWP
   1. замените параметр puserss на BWREAD в варианте программы
   2. очистите переключатель pkeepcsv в варианте программы
   3. скорректируйте SELECT, если меняли его на шаге 4
   4. удалите zip-файлы c данными (в AL11 или другим способом, если этого не сделал процесс, запускаемый дата-инженерами)
7. Тестирование на продуктивных данных в системе разработки завершено. Можно нести в продуктив.

Для запуска hdbsql на вашей рабочей станции рекомендуется использовать bat-файл, сформированный по образцу (см ниже). В нем заменить путь к имени
файла с sql-select-запросом, путь к файлу с csv-данными (результатом выполнения) и имени ключа в usersecstore (ключ создается утилитой hdbuserstore)

``/doc/runhdbsql.bat``

## Селекционный экран программы
Программа имеет следующие параметры селекционного экрана:
* pclass TYPE char605 OBLIGATORY DEFAULT 'ZCL_BW_HDBSQLCSV_GXXXXSXXX', " Класс с определением SELECT-выражения для выгрузки
* psetg TYPE zhdbsqlg OBLIGATORY LOWER CASE DEFAULT 'gXXXX', " Имя группы csv-выгрузок
* pset TYPE zhdbsqls OBLIGATORY LOWER CASE DEFAULT 'sXXX', " Имя набора (внутри группы) csv-выгрузок
* psfrname TYPE char605 LOWER CASE, " имя файла для скриптов и выгрузок (без метки времени)
* prootfld TYPE char605 LOWER CASE DEFAULT '/mnt/hdbsqlexp/hdbsql', " имя корневой папки на сервере BW для хранения всех скриптов
* puserss TYPE char151 DEFAULT 'BWREAD', " имя записи в UserSecureStore для подключения к HANA из hdbsql
* pttimest TYPE char1 DEFAULT '1', " код типа метки времени (0 - нет, 1 - дата, 2 - дата_время
* pkeepcsv TYPE char1 DEFAULT '', " флаг сохранения csv-файлов после архивирования zip
* psaveloc TYPE char1 DEFAULT ''. " флаг необходимости делать скрипты для запуска на локальном компьютере Windows

При выборе pkeepcsv = X после формирование zip-файла выполняются удаления sh-, csv- и def-файлов. Остаются только zip-файл (в подкаталоге zip) и sql-файл
(с текстом SELECT-запроса для выгрузки), в каталоге arch. ZIP-файлы должен забирать пользователь hadoop И удалять после получения. SQL-файлы
накапливаются в каталоге для истории выгрузок и удаляются (созданные ранее 30 дней) автоматически процессом на AIX.

Вместо BWREAD можно указать BWREADZ, что позволит запускать выгрузки под пользователем с большей квотой на использование оперативной памяти в
HANA. Размер квоты оперативной памяти в SAP HANA определяется привоенным пользователю workload class. Это, с одной стороны, позволит избежать OOM-ошибок в вашей выгрузке, но может привести к OOM-ошибкам в других процессах, если (и только если) общей памяти в HANA будет не хватать. Используйте BWREADZ только в случаях крайней необходимости.