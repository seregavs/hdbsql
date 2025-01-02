class ZCL_BW_HDBSQLCSV_G0009S001 definition
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
  class-methods INSERT_PLANT_DATA
    importing
      !IN_BEGP type /BI0/OIPLANT
      !IN_ENDP type /BI0/OIPLANT .

  methods BEFORE_RUN_HDBSQL
    redefinition .
  PROTECTED SECTION.
    METHODS set_select_script REDEFINITION.
    METHODS get_delta_text REDEFINITION.
    METHODS set_select_script_tt REDEFINITION.
private section.

  class-methods GET_MONTHS_BACK
    importing
      !IN_N type NUMC2
    returning
      value(O_RES) type CHAR100 .
  methods BKGR_PROC_FACTORY .
ENDCLASS.



CLASS ZCL_BW_HDBSQLCSV_G0009S001 IMPLEMENTATION.


  METHOD constructor.
    super->constructor( i_setgroup = i_setgroup i_set = i_set
                        i_set_friname = i_set_friname i_rootfolder = i_rootfolder
                        i_usersstore = i_usersstore i_ttimestamp = i_ttimestamp i_keepcsv = i_keepcsv ).


  ENDMETHOD.


  METHOD before_run_hdbsql.
    DATA: lv_plant TYPE /bi0/oiplant.
    DATA(lv_delta) = me->get_delta( ).
    CONDENSE lv_delta NO-GAPS.

    IF ( lv_delta = '' ).
      WRITE: / | No before action was executed |.
      RETURN.
    ENDIF.

    DELETE FROM zmatplant_abc.
    WRITE: / | ZMATPLANT_ABC was emptied |.

    IF ( lv_delta = 'ALL' ).
      "me->insert_plant_data( in_begp = '0001' in_endp = '9999' ).
      me->bkgr_proc_factory( ).
    ELSE.
      lv_plant = lv_delta+0(4).
      me->insert_plant_data( in_begp = lv_plant in_endp = lv_plant ).
    ENDIF.
    WRITE: / | ZMATPLANT_ABC was filled OK |.
  ENDMETHOD.


  METHOD get_delta_text.
    o_res = |_{ me->get_delta( ) }|.
  ENDMETHOD.


  METHOD set_select_script.
    " Полная выгрузка счетов-фактур из CV lenta.bw.rt.log.sal/BASE_SD_AGGR_CV01
    " помесячная, интервал в днях указывается в настроечной таблице
    DATA: lv_row    TYPE ty_string.
    DATA(lv_d) = sy-datum.

    "lv_row-row = |SELECT * FROM "_SYS_BIC"."system-local.bw.bw2hana/ZHER_WGH4";|.
*  lv_row-row = |SELECT session_context('LOCALE_SAP') as locale_sap, session_context('LOCALE') as locale, | &
*               |  session_context('APPLICATIONUSER') as app_user, current_user FROM dummy;|.

    lv_row-row = |SELECT MATERIAL, PLANT, ZABC, '{ lv_d }' as LOADDATE FROM "{ me->i_dbschema }".ZMATPLANT_ABC ORDER BY 2,1,3;|.
    APPEND lv_row TO me->i_select_script.
  ENDMETHOD.


  METHOD set_select_script_tt.
    DATA: lv_row    TYPE ty_string.
    CLEAR: me->i_select_script_tt.
    " no ; at the end of the statement !!!
    lv_row-row = |SELECT MATERIAL, PLANT, ZABC, '99991231' as LOADDATE FROM "{ me->i_dbschema }".ZMATPLANT_ABC WHERE 1 = 2|.
    APPEND lv_row TO me->i_select_script_tt.
  ENDMETHOD.


  METHOD bkgr_proc_factory.
    " text = |{ CONV string( num ) WIDTH = 10 ALPHA = IN }|.
    TYPES:
      BEGIN OF t_plant_int,
        begp TYPE /bi0/oiplant,
        endp TYPE /bi0/oiplant,
      END OF t_plant_int,
      tt_plant_int TYPE STANDARD TABLE OF t_plant_int.

    DATA:
      lt_plant_int TYPE tt_plant_int,
      lv_repname   TYPE edpline.

    TYPES:
      BEGIN OF t_job,
        jobname   TYPE btcjob,
        jobnumber TYPE btcjobcnt,
      END OF t_job,
      tt_job TYPE STANDARD TABLE OF t_job.

    DATA: rspar            TYPE TABLE OF rsparams,
          lv_jobname       TYPE btcjob,
          lv_jobnumber     TYPE btcjobcnt,
          print_parameters TYPE pri_params,
          lt_job           TYPE tt_job,
          lt_code          TYPE TABLE OF rssource-line.

    APPEND VALUE #( begp = '0001' endp = '0050' ) TO lt_plant_int.
    APPEND VALUE #( begp = '0051' endp = '0110' ) TO lt_plant_int.
    APPEND VALUE #( begp = '0111' endp = '0166' ) TO lt_plant_int.
    APPEND VALUE #( begp = '0167' endp = '0228' ) TO lt_plant_int.
    APPEND VALUE #( begp = '0229' endp = '0309' ) TO lt_plant_int.
    APPEND VALUE #( begp = '0310' endp = '0849' ) TO lt_plant_int.
    APPEND VALUE #( begp = '0850' endp = '1420' ) TO lt_plant_int.
    APPEND VALUE #( begp = '1421' endp = '3570' ) TO lt_plant_int.
    APPEND VALUE #( begp = '3571' endp = '9999' ) TO lt_plant_int.

    IF 1 = 2. " Report generation blocked temporarily
      DATA(lo_ran) = cl_abap_random_int=>create( seed = CONV i( sy-uzeit )
                                        min  = 1
                                        max = 100 ).
      DATA(lv_i) = lo_ran->get_next( ).
      lv_repname = |ZPROC_G0009S001_{ lv_i }|.
      CLEAR: lt_code.
      APPEND  |REPORT { lv_repname }.| TO lt_code.
      APPEND  'PARAMETERS: pbegp TYPE /bi0/oiplant OBLIGATORY,' TO lt_code.
      APPEND  '            pendp TYPE /bi0/oiplant OBLIGATORY.' TO lt_code.
      APPEND  | ZCL_BW_HDBSQLCSV_G0009S001=>INSERT_PLANT_DATA( | TO lt_code.
      APPEND  |  in_begp = pbegp in_endp = pendp ).| TO lt_code.
      APPEND  ' WRITE: / | begin_plant = { pbegp } ed_plant = { pendp }|.' TO lt_code.
      INSERT REPORT lv_repname FROM lt_code.
      WAIT UP TO 2 SECONDS.
    ELSE.
      lv_repname = |ZPROC_G0009S001|.
    ENDIF.

    CLEAR: lt_job.
    LOOP AT lt_plant_int ASSIGNING FIELD-SYMBOL(<fs>).
      rspar = VALUE #(
       ( selname = 'PBEGP'
         kind = 'P'
         low  = <fs>-begp )
       ( selname = 'PENDP'
         kind = 'P'
         low  = <fs>-endp ) ).
      lv_jobname = |HDBSQLG0009S001_{ <fs>-begp }_{ <fs>-endp }|.
      CALL FUNCTION 'JOB_OPEN'
        EXPORTING
          jobname          = lv_jobname
        IMPORTING
          jobcount         = lv_jobnumber
        EXCEPTIONS
          cant_create_job  = 1
          invalid_job_data = 2
          jobname_missing  = 3
          OTHERS           = 4.
      IF sy-subrc = 0.
        APPEND VALUE #( jobname = lv_jobname jobnumber = lv_jobnumber ) TO lt_job.
        SUBMIT (lv_repname) TO SAP-SPOOL
                        SPOOL PARAMETERS print_parameters
                        WITHOUT SPOOL DYNPRO
                        VIA SELECTION-SCREEN
                        WITH SELECTION-TABLE rspar
                        VIA JOB lv_jobname NUMBER lv_jobnumber
                        AND RETURN.
        IF sy-subrc = 0.
          WRITE: / |Job { lv_jobname } started|.
          WAIT UP TO 1 SECONDS.
          CALL FUNCTION 'JOB_CLOSE'
            EXPORTING
              jobcount             = lv_jobnumber
              jobname              = lv_jobname
              strtimmed            = 'X'
            EXCEPTIONS
              cant_start_immediate = 1
              invalid_startdate    = 2
              jobname_missing      = 3
              job_close_failed     = 4
              job_nosteps          = 5
              job_notex            = 6
              lock_failed          = 7
              OTHERS               = 8.
          IF sy-subrc <> 0.
            WRITE: / |Error opening job { lv_jobname } subrc = { sy-subrc }|.
            MESSAGE  |Error opening job { lv_jobname } subrc = { sy-subrc }| TYPE 'S'.
          ENDIF.
        ELSE.
          MESSAGE |Error submitting { lv_repname } subrc = { sy-subrc }| TYPE 'S'.
        ENDIF.
      ELSE.
        MESSAGE |Error opening job { lv_jobname } subrc = { sy-subrc }| TYPE 'S'.
      ENDIF.
      MESSAGE |Plants { <fs>-begp } - { <fs>-endp } loaded.| TYPE 'S'.
    ENDLOOP.

    " do check in almost infinite cycle of job execution
    DO.
      IF lt_job IS NOT INITIAL.
        SELECT COUNT(*) AS cnt FROM tbtco
          FOR ALL ENTRIES IN @lt_job
          WHERE jobname  = @lt_job-jobname
            AND jobcount = @lt_job-jobnumber
          AND ( status IN ('A','F') )
         INTO ( @DATA(lv_cnt) ).
        IF lv_cnt = lines( lt_job ).
          DATA(lv_mess) = | All { lines( lt_job ) } jobs were completed|.
          WRITE: / lv_mess.
          MESSAGE lv_mess TYPE 'S'.
          EXIT.
        ELSE.
          lv_mess = | Only { lv_cnt } of { lines( lt_job ) } jobs were completed|.
          WRITE: / lv_mess.
          MESSAGE lv_mess TYPE 'S'.
        ENDIF.
        WAIT UP TO 10 SECONDS.
      ENDIF.
    ENDDO.

*Values for TBTCO-STATUS:
*
*A - Cancelled
*F - Completed
*P - Scheduled
*R - Active
*S - Released
  ENDMETHOD.


  METHOD get_months_back.
    DATA(lv_cm) = CONV rscalmonth( sy-datum+0(6) ).
    DO in_n TIMES.
      lv_cm = NEW zclbw_zhr_date_utils( i_calmonth = lv_cm )->get_calmonth_shifted( i_shift_in_months = -1 ).
      o_res = |{ lv_cm },{ o_res }|.
    ENDDO.
    IF strlen( o_res ) > 0.
      o_res = substring( val = o_res off = 0 len = strlen( o_res ) - 1 ).
    ENDIF.
*    CASE lv_cm.
*      WHEN '202212'.
*        o_res = '202209,202210,202211'.
*      WHEN '202301'.
*        o_res = '202210,202211,202212'.
*      WHEN '202302'.
*        o_res = '202211,202212,202301'.
*      WHEN OTHERS.
*        o_res = '202212,202301,202302'.
*    ENDCASE.
  ENDMETHOD.


  METHOD insert_plant_data.

    DATA:
      lo_conn      TYPE REF TO cl_sql_connection,
      lo_statement TYPE REF TO cl_sql_statement,
      lx_sql       TYPE REF TO cx_sql_exception,
      lt_res       TYPE REF TO data.

    TYPES:
      BEGIN OF t_plant,
        plant TYPE /bi0/oiplant,
      END OF t_plant,
      tt_plant TYPE STANDARD TABLE OF t_plant.
    DATA:
      lt_plant TYPE tt_plant,
      m1(6)    TYPE c,
      m2(6)    TYPE c,
      m3(6)    TYPE c.

    SELECT plant INTO TABLE @lt_plant FROM /bi0/mplant
     WHERE objvers = 'A' AND plantcat = 'A'
       AND dateto = '99991231'
       AND /bic/zfrmttyp <> ''
       AND plant BETWEEN @in_begp AND @in_endp
     ORDER BY plant ASCENDING.

    DATA(lv_3months) = zcl_bw_hdbsqlcsv_g0009s001=>get_months_back( 3 ).
    SPLIT lv_3months AT ',' INTO m1 m2 m3.
    DATA(lv_monthstr) = |'''{ m1 }'',''{ m2 }'',''{ m3 }'''|.

    LOOP AT lt_plant ASSIGNING FIELD-SYMBOL(<fs_plant>).
      DATA(lv_stmt) = |INSERT INTO "ZMATPLANT_ABC" (MANDT, MATERIAL, PLANT, ZABC ) (| &
      | SELECT { sy-mandt }, "MATERIAL","PLANT", "ABCKEY" | &
      |    FROM "_SYS_BIC"."lenta.bw.lspec.abc/ABC_CV01_HRCP"('PLACEHOLDER' = ('$$ip_base_uom$$', '-'), | &
      |     'PLACEHOLDER' = ('$$ip_zabckf$$','RTSAEXCUST'),| &
      |     'PLACEHOLDER' = ('$$ip_rpa_wgh3$$', '''-'''),| &
      |     'PLACEHOLDER' = ('$$ip_vendor$$', '''-'''),| &
      |     'PLACEHOLDER' = ('$$ip_plntclist$$', '''-'''),| &
      |     'PLACEHOLDER' = ('$$ip_country$$','-'),| &
      |     'PLACEHOLDER' = ('$$ip_calyear$$', '''-'''),| &
      |     'PLACEHOLDER' = ('$$ip_calquarter$$', '''-'''),| &
      |     'PLACEHOLDER' = ('$$ip_calmonth$$', { lv_monthstr }),| &
      |     'PLACEHOLDER' = ('$$ip_comp_code$$', '''-'''),| &
      |     'PLACEHOLDER' = ('$$ip_calweek$$', '''-'''),| &
      |     'PLACEHOLDER' = ('$$ip_zprclgrp$$', '-'),| &
      |     'PLACEHOLDER' = ('$$ip_zabcvar$$', '01'),| &
      |     'PLACEHOLDER' = ('$$ip_salesorg$$', '-'),| &
      |     'PLACEHOLDER' = ('$$ip_abcreptyp$$', 'MC'),| &
      |     'PLACEHOLDER' = ('$$ip_distr_chan$$', '''-'''),| &
      |     'PLACEHOLDER' = ('$$ip_rpa_wgh4$$', '''-'''),| &
      |     'PLACEHOLDER' = ('$$ip_plant$$', '''{ <fs_plant>-plant }'''),| &
      |     'PLACEHOLDER' = ('$$ip_purch_org$$', '''-'''),| &
      |     'PLACEHOLDER' = ('$$ip_city_code$$', '-'),| &
      |     'PLACEHOLDER' = ('$$ip_zsaletype$$', '''-''')) );|.
      TRY.
          lo_conn = cl_sql_connection=>get_connection( ).
          lo_statement = lo_conn->create_statement( ).
          DATA(l_row_cnt) = lo_statement->execute_update( lv_stmt ).
          COMMIT WORK.
          DATA(lv_mess) = |{ sy-tabix } plant={ <fs_plant>-plant }, rows inserted={ l_row_cnt }|.
          WRITE: / lv_mess.
          MESSAGE lv_mess TYPE 'S'.
        CATCH cx_sql_exception INTO lx_sql.
          lv_mess = |plant={ <fs_plant>-plant } Err text: { lx_sql->get_text( ) }|.
          WRITE: / lv_mess.
          MESSAGE lv_mess TYPE 'S'.
          lv_mess = |plant={ <fs_plant>-plant } SQL Code: { lx_sql->sql_code }|.
          WRITE: / lv_mess.
          MESSAGE lv_mess TYPE 'S'.
          lv_mess = lx_sql->sql_message.
          WRITE: / lv_mess.
          MESSAGE lv_mess TYPE 'S'.
          RETURN.
      ENDTRY.
    ENDLOOP.

  ENDMETHOD.
ENDCLASS.