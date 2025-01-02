*&---------------------------------------------------------------------*
*& Report ZBW_HDBSQL_CSV
*&---------------------------------------------------------------------*
*&
*&---------------------------------------------------------------------*
REPORT  zbw_hdbsql_csv.

PARAMETERS:
  pclass   TYPE char605 OBLIGATORY DEFAULT 'ZCL_BW_HDBSQLCSV_GXXXXSXXX', " Класс с определением SELECT-выражения для выгрузки
  psetg    TYPE zhdbsqlg OBLIGATORY LOWER CASE DEFAULT 'gXXXX', " Имя группы csv-выгрузок
  pset     TYPE zhdbsqls OBLIGATORY LOWER CASE DEFAULT 'sXXX', " Имя набора (внутри группы) csv-выгрузок
  psfrname TYPE char605 LOWER CASE, " имя файла для скриптов и выгрузок (без метки времени)
  prootfld TYPE char605 LOWER CASE DEFAULT '/mnt/hdbsqlexp/hdbsql', " имя корневой папки на сервере BW для хранения всех скриптов
  puserss  TYPE char151 DEFAULT 'BWREAD', " имя записи в UserSecureStore для подключения к HANA из hdbsql
  pttimest TYPE char1 DEFAULT '1', " код типа метки времени (0 - нет, 1 - дата, 2 - дата_время
  pkeepcsv TYPE char1 DEFAULT '', " флаг сохранения csv-файлов после архивирования zip
  psaveloc TYPE char1 DEFAULT ''. " флаг сохранения скриптов ТОЛЬКО для запуска на локальном компьютере Windows


*WRITE: / |ABAP-class = { pclass }|.
*WRITE: / |group = { psetg }|.
*WRITE: / |set = { pset }|.
*WRITE: / |file name without timestamp = { psfrname }|.
*WRITE: / |root folder for group = { prootfld }|.
*WRITE: / |user secure store = { puserss }|.
*WRITE: / |type of timestamp for file name ('' no, '1' date, '2' datetime) = { pttimest }|.
*WRITE: / |do not delete csv after zip = { pkeepcsv }|.
*WRITE: /.

*DATA(lv_date) = CONV dats( '20140929' )

DATA: lo_hdbsql TYPE REF TO zcl_bw_hdbsqlcsv_base.

" infinite loop for debugging. Disabled for now.
DATA: lv_i TYPE i.
WHILE lv_i = 0.
  lv_i = 1.
ENDWHILE.

TRY.
    CREATE OBJECT lo_hdbsql
        TYPE (pclass)
        EXPORTING
          i_setgroup = psetg
          i_set = pset
          i_set_friname = psfrname
          i_ttimestamp = pttimest
          i_rootfolder = prootfld
          i_usersstore = puserss
          i_keepcsv = pkeepcsv.
  CATCH cx_sy_create_object_error.
    WRITE: / |Incorrect class name { pclass }|.
    RAISE incorrect_class_name.
ENDTRY.

IF lo_hdbsql IS NOT BOUND.
  WRITE: / |Incorrect class name { pclass }|.
  RAISE create_instance_error.
ENDIF.

IF psaveloc = 'X'.
  lo_hdbsql->make_primary_script_loc( ).
ELSE.
  lo_hdbsql->make_primary_script(  ).
ENDIF.
lo_hdbsql->before_run_hdbsql( ).
*lo_hdbsql->print_scripts( ).
*WRITE: / |All done.|.