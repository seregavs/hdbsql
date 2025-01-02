class ZCL_BW_HDBSQLCSV_G0002S001 definition
  public
  inheriting from ZCL_BW_HDBSQLCSV_BASE
  final
  create public .

public section.

  methods CONSTRUCTOR
    importing
      !I_SETGROUP type ZHDBSQLG
      !I_SET type ZHDBSQLS
      !I_SET_FRINAME type CHAR605 optional
      !I_ROOTFOLDER type CHAR605 default '/home/tempuser/hdbsql'
      !I_USERSSTORE type CHAR151 default 'SSHABHP'
      !I_TTIMESTAMP type CHAR1 default '1'
      !I_KEEPCSV type CHAR1 default '' .
    METHODS UPDATE_MISSED REDEFINITION.
protected section.
  methods set_select_script redefinition.
  METHODS get_delta_text REDEFINITION.
  methods SET_SELECT_SCRIPT_TT REDEFINITION.
private section.
ENDCLASS.



CLASS ZCL_BW_HDBSQLCSV_G0002S001 IMPLEMENTATION.


METHOD CONSTRUCTOR.
  super->constructor( i_setgroup = i_setgroup i_set = i_set
                      i_set_friname = i_set_friname i_rootfolder = i_rootfolder
                      i_usersstore = i_usersstore i_ttimestamp = i_ttimestamp i_keepcsv = i_keepcsv ).

  "me->i_set_friname = |{ me->i_set_friname }{ me->i_timestamp }|.
ENDMETHOD.


METHOD update_missed.
  " ZHDBSQLLOG
  DATA: lv_begda TYPE d,
        lv_endda TYPE d.
  DATA(lv_d) = sy-datum.

  "add to hdbsql_log data from change log
  DATA(lv_stmt) =
   | SELECT DISTINCT calday FROM ( | &
   |  SELECT DISTINCT left(reqtsn,8) as reqdate, calday FROM "{ me->i_dbschema }"."/BIC/AZSD10DC431" UNION ALL | &
   |  SELECT DISTINCT left(reqtsn,8) as reqdate, calday FROM "{ me->i_dbschema }"."/BIC/AZSDXXDC021") | &
   |   WHERE reqdate >= '{ lv_d }')|.


  "remove hdbsql_log data extracted to Hadoop
  lv_d = sy-datum.
  SELECT * INTO @DATA(ls_gs) FROM zhdbsqlgs
    WHERE g = @me->i_setgroup
      AND s = @me->i_set
      AND status = 'A'.
    " defining date interval for gs_position (active (status = 'A' only)
    IF ls_gs-delta = ''.
      lv_endda = lv_d.
      lv_d = lv_d - 2.
      lv_begda = lv_d.
    ELSE.
      SPLIT ls_gs-delta AT ';' INTO lv_begda lv_endda.
      IF lv_begda IS INITIAL.
        lv_endda = lv_d.
        lv_d = lv_d - 2.
        lv_begda = lv_d.
      ELSE.
        IF lv_endda IS INITIAL.
          lv_endda = lv_begda.
        ENDIF.
      ENDIF.
    ENDIF.
    " removing missed data because the datas were loaded
    WHILE lv_begda <= lv_endda.
      DELETE FROM zhdbsqlmiss
       WHERE g = @me->i_setgroup AND s = @me->i_set
         AND DATA1 = @lv_begda.
      lv_begda = lv_begda + 1.
    ENDWHILE.


  ENDSELECT.
ENDMETHOD.


METHOD set_select_script.
  " Полная выгрузка счетов-фактур из CV lenta.bw.rt.log.sal/BASE_SD_AGGR_CV01
  " помесячная, интервал в днях указывается в настроечной таблице
  DATA: lv_row    TYPE ty_string.
  DATA: lv_begda TYPE sy-datum,
        lv_endda TYPE sy-datum,
        lv_c type char3.
  CLEAR: me->i_select_script.

  lv_c = me->get_delta( ).
  DATA(lv_d) =  sy-datum.
  lv_endda = lv_d - lv_c.
  lv_begda = lv_endda - 2.


  IF lv_c = ''.
    lv_endda = lv_d.
    lv_d = lv_d - 2.
    lv_begda = lv_d.
*  ELSE.
*    SPLIT lv_delta AT ';' INTO lv_begda lv_endda.
*    IF lv_begda IS INITIAL.
*      lv_endda = lv_d.
*      lv_d = lv_d - 2.
*      lv_begda = lv_d.
*    ELSE.
*      IF lv_endda IS INITIAL.
*        lv_endda = lv_begda.
*      ENDIF.
*    ENDIF.
  ENDIF.
  lv_row-row = | SELECT | &
   | ifnull(t1."CALDAY",'') as "CALDAY", | &
   | ifnull("BASE_UOM",'') as "BASE_UOM", | &
   | ifnull("SALES_UNIT",'') as "SALES_UNIT", | &
   | ifnull("PLANT",'') as "PLANT", | &
   | ifnull("DISTR_CHAN",'') as "DISTR_CHAN", | &
   | ifnull("LOC_CURRCY",'') as "LOC_CURRCY", | &
   | ifnull("ZSALETYPE",'') as "ZSALETYPE", | &
   | ifnull("MATERIAL",'') as "MATERIAL", | &
   | ifnull("DIVISION",'') as "DIVISION", | &
   | ifnull("0DIVISION__ZVGOTYPE",'') as "0DIVISION__ZVGOTYPE", | &
   | ifnull("ZFLAGPRO",'') as "ZFLAGPRO", | &
   | ifnull("RT_DEPARTM",'') as "RT_DEPARTM", | &
   | ifnull("VENDOR",'') as "VENDOR", |.
  APPEND lv_row TO me->i_select_script.
  lv_row-row = | ifnull("PLANTCAT",'') as "PLANTCAT", | &
  | ifnull("SALESORG",'') as "SALESORG", | &
  | ifnull("FCTVENDOR",'') as "FCTVENDOR", | &
  | ifnull("0PLANT_ZFRMTTYP",'') as "0PLANT_ZFRMTTYP", | &
  | ifnull("0MAT_PLANT__CTYPEMATR",'') as "0MAT_PLANT__CTYPEMATR", | &
  | ifnull("ZINSTYPE",'') as "ZINSTYPE", | &
  | ifnull("0MAT_PLANT__ZSERVG",'') as "0MAT_PLANT__ZSERVG", | &
  | ifnull("ZFRMTAGG",'') as "ZFRMTAGG" ,| &
  | ifnull("0PCOMPANY",'') as "0PCOMPANY", |.
  APPEND lv_row TO me->i_select_script.
  lv_row-row =  | SUM("CPSAEXCUBU") AS "CPSAEXCUBU", | &
  | SUM("CPSAEXCUPV") AS "CPSAEXCUPV", | &
  | SUM("CPSAEXCUSU") AS "CPSAEXCUSU", | &
  | SUM("RTSAEXCUST") AS "RTSAEXCUST", | &
  | SUM("RTSAEXCUSV") AS "RTSAEXCUSV", | &
  | SUM("ZALTCOST") AS "ZALTCOST", | &
  | SUM("ZALTCSTBN") AS "ZALTCSTBN", | &
  | SUM("ZDEVSCOST") AS "ZDEVSCOST" |.
  APPEND lv_row TO me->i_select_script.
  lv_row-row = | FROM "_SYS_BIC"."lenta.bw.rt.log.sal/BASE_SD_AGGR_CV01" t1 | &
  | WHERE t1.calday BETWEEN '{ lv_begda }' AND '{ lv_endda }' | &
  | GROUP BY t1."CALDAY", "BASE_UOM", "SALES_UNIT","PLANT","DISTR_CHAN","LOC_CURRCY","ZSALETYPE", "MATERIAL", "DIVISION", | &
  | "0DIVISION__ZVGOTYPE","ZFLAGPRO","RT_DEPARTM","VENDOR", "PLANTCAT", "SALESORG","FCTVENDOR", "0PLANT_ZFRMTTYP", "0MAT_PLANT__CTYPEMATR",| &
  | "ZINSTYPE", "0MAT_PLANT__ZSERVG","ZFRMTAGG", "0PCOMPANY"|.
"  ||.
  APPEND lv_row TO me->i_select_script.
ENDMETHOD.


METHOD get_delta_text.
  DATA: lv_begda TYPE sy-datum,
        lv_endda TYPE sy-datum,
        lv_c(4)  TYPE n. " fixed. 4-digit offset should be enough. Otherwise must be extended.

  lv_c = me->get_delta( ).
  DATA(lv_d) =  sy-datum.
  lv_endda = lv_d - lv_c.
  lv_begda = lv_endda - 2.

  o_res = |_{ lv_begda }to{ lv_endda }|.

ENDMETHOD.


  method SET_SELECT_SCRIPT_TT.
    DATA: lv_row    TYPE ty_string.
    CLEAR: me->i_select_script_tt.
    " no ; at the end of the statement !!!
  lv_row-row = | SELECT | &
   | t1."CALDAY", | &
   | "BASE_UOM", | &
   | "SALES_UNIT", | &
   | "PLANT", | &
   | "DISTR_CHAN", | &
   | "LOC_CURRCY", | &
   | "ZSALETYPE", | &
   | "MATERIAL", | &
   | "DIVISION", | &
   | "0DIVISION__ZVGOTYPE", | &
   | "ZFLAGPRO", | &
   | "RT_DEPARTM", | &
   | "VENDOR", |.
  APPEND lv_row TO me->i_select_script_tt.
   lv_row-row = | "PLANTCAT", | &
   | "SALESORG", | &
   | "FCTVENDOR", | &
   | "0PLANT_ZFRMTTYP", | &
   | "0MAT_PLANT__CTYPEMATR", | &
   | "ZINSTYPE", | &
   | "0MAT_PLANT__ZSERVG", | &
   | "ZFRMTAGG", | &
   | "0PCOMPANY", |.
    APPEND lv_row TO me->i_select_script_tt.
   lv_row-row =  | ("CPSAEXCUBU") AS "CPSAEXCUBU", | &
   | ("CPSAEXCUPV") AS "CPSAEXCUPV", | &
   | ("CPSAEXCUSU") AS "CPSAEXCUSU", | &
   | ("RTSAEXCUST") AS "RTSAEXCUST", | &
   | ("RTSAEXCUSV") AS "RTSAEXCUSV", | &
   | ("ZALTCOST") AS "ZALTCOST", | &
   | ("ZALTCSTBN") AS "ZALTCSTBN", | &
   | ("ZDEVSCOST") AS "ZDEVSCOST" |.
  APPEND lv_row TO me->i_select_script_tt.
   lv_row-row =  | FROM "_SYS_BIC"."lenta.bw.rt.log.sal/BASE_SD_AGGR_CV01" t1 | &
   | WHERE t1.calday BETWEEN '19000101' AND '19000102' AND 1 = 2 |.
   APPEND lv_row TO me->i_select_script_tt.
  endmethod.
ENDCLASS.