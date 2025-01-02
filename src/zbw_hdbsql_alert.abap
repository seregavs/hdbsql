" commented because becomes obsolete

*    DATA: file      TYPE string.
*
*    DATA(lv_fn1) =  me->i_set_friname1.
*    REPLACE FIRST OCCURRENCE OF '_@' IN lv_fn1 WITH '' IN CHARACTER MODE.
*
*    file = |{ me->get_folder( ) }/zip/{ lv_fn1 }.zip|.
*    TRY.
*        OPEN DATASET file FOR OUTPUT IN TEXT MODE ENCODING DEFAULT WITH SMART LINEFEED.
*        IF sy-subrc <> 0.
*          WRITE: / |Dataset for file { file } has errors while opening|.
*        ENDIF.
*        TRANSFER 'Data in BW is ready for extraction' TO file.
*        CLOSE DATASET file.
*        WRITE: / | File created: { file }|.
*      CATCH cx_root INTO DATA(lx_root).
*        DATA(lv_err) = lx_root->kernel_errid.
*        WRITE: / | Error creating { file }: { lv_err }|.
*    ENDTRY.


*&---------------------------------------------------------------------*
*& Report ZBW_HDBSQL_ALERT
*&---------------------------------------------------------------------*
*& Alert file is dummy file with extention zip. It has no information
*& but whether the file is in the catalog, it indicates that data in BW is ready
*& for taking by extarnal system (e.g. AirFlow DAGs)
*&---------------------------------------------------------------------*
REPORT  zbw_hdbsql_alert.

PARAMETERS:
  pclass   TYPE char605 OBLIGATORY DEFAULT 'ZCL_BW_HDBSQLCSV_GXXXXSXXX', " Класс с определением SELECT-выражения для выгрузки
  psetg    TYPE zhdbsqlg OBLIGATORY LOWER CASE DEFAULT 'gXXXX', " Имя группы csv-выгрузок
  pset     TYPE zhdbsqls OBLIGATORY LOWER CASE DEFAULT 'sXXX', " Имя набора (внутри группы) csv-выгрузок
  psfrname TYPE char605 LOWER CASE, " имя файла для скриптов и выгрузок (без метки времени)
  prootfld TYPE char605 LOWER CASE DEFAULT '/mnt/hdbsqlexp/hdbsql'. " имя корневой папки на сервере BW для хранения всех скриптов

*DATA(lv_date) = CONV dats( '20140929' )

DATA: lo_hdbsql TYPE REF TO zcl_bw_hdbsqlcsv_base.

TRY.
    CREATE OBJECT lo_hdbsql
        TYPE (pclass)
        EXPORTING
          i_setgroup = psetg
          i_set = pset
          i_set_friname = psfrname
          i_ttimestamp = '1' "pttimest
          i_rootfolder = prootfld
          i_usersstore = 'BWREAD'
          i_keepcsv = ''. " pkeepcsv.
  CATCH cx_sy_create_object_error.
    WRITE: / |Incorrect class name { pclass }|.
    RAISE incorrect_class_name.
ENDTRY.

IF lo_hdbsql IS NOT BOUND.
  WRITE: / |Incorrect class name { pclass }|.
  RAISE create_instance_error.
ENDIF.
lo_hdbsql->make_alert_file( ).