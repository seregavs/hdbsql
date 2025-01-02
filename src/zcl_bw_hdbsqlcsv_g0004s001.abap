class ZCL_BW_HDBSQLCSV_G0004S001 definition
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
protected section.
  methods set_select_script redefinition.
  METHODS get_delta_text REDEFINITION.
  methods SET_SELECT_SCRIPT_TT REDEFINITION.
private section.
ENDCLASS.



CLASS ZCL_BW_HDBSQLCSV_G0004S001 IMPLEMENTATION.


METHOD CONSTRUCTOR.
  super->constructor( i_setgroup = i_setgroup i_set = i_set
                      i_set_friname = i_set_friname i_rootfolder = i_rootfolder
                      i_usersstore = i_usersstore i_ttimestamp = i_ttimestamp i_keepcsv = i_keepcsv ).


ENDMETHOD.


method GET_DELTA_TEXT.
  o_res = ''.
ENDMETHOD.


METHOD set_select_script.
  " Полная выгрузка счетов-фактур из CV lenta.bw.rt.log.sal/BASE_SD_AGGR_CV01
  " помесячная, интервал в днях указывается в настроечной таблице
  DATA: lv_row    TYPE ty_string.

  "lv_row-row = |SELECT * FROM "_SYS_BIC"."system-local.bw.bw2hana/ZHER_WGH4";|.
*  lv_row-row = |SELECT session_context('LOCALE_SAP') as locale_sap, session_context('LOCALE') as locale, | &
*               |  session_context('APPLICATIONUSER') as app_user, current_user FROM dummy;|.
  DATA(lv_date) = sy-datum.
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
 " | '{ lv_date }' as LOADDATE | &
  | FROM "_SYS_BIC"."system-local.bw.bw2hana/ZHER_WGH4" ORDER BY "ZHER_WGH4" ASC;|.
  APPEND lv_row TO me->i_select_script.
ENDMETHOD.


  METHOD set_select_script_tt.
    DATA: lv_row    TYPE ty_string.
    CLEAR: me->i_select_script_tt.
    " no ; at the end of the statement !!!
    lv_row-row = | SELECT * FROM "_SYS_BIC"."system-local.bw.bw2hana/ZHER_WGH4" WHERE 1 = 2|.
    APPEND lv_row TO me->i_select_script_tt.
  ENDMETHOD.
ENDCLASS.