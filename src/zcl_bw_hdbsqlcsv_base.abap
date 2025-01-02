CLASS zcl_bw_hdbsqlcsv_base DEFINITION
  PUBLIC
  CREATE PUBLIC .

PUBLIC SECTION.

  TYPES:
    BEGIN OF ty_string,
        row TYPE string,
      END OF ty_string,
    BEGIN OF ENUM ty_util BASE TYPE char4,
        zip  VALUE 'zip',
        gzip VALUE 'gz',
        init VALUE IS INITIAL,
    END OF ENUM ty_util.
  TYPES:
    tty_string TYPE STANDARD TABLE OF ty_string WITH DEFAULT KEY .

  METHODS constructor
    IMPORTING
      !i_setgroup TYPE zhdbsqlg
      !i_set TYPE zhdbsqls
      !i_set_friname TYPE char605 OPTIONAL
      !i_rootfolder TYPE char605 DEFAULT '/home/tempuser/hdbsql'
      !i_usersstore TYPE char151 DEFAULT 'BWREAD'
      !i_ttimestamp TYPE char1 DEFAULT '1'
      !i_keepcsv TYPE char1 DEFAULT '' .
  METHODS make_hdbsql_script .
  METHODS make_select_script .
  METHODS make_hdbsql_script_loc .
  METHODS make_select_script_loc .
  METHODS print_scripts .
  METHODS make_ddef_script .
  METHODS make_ddef_script_loc .
  METHODS make_primary_script .
  METHODS make_primary_script_loc .
  METHODS before_run_hdbsql .
  METHODS after_run_hdbsql .
  METHODS make_log .
  METHODS update_missed .
PROTECTED SECTION.

  DATA i_setgroup TYPE zhdbsqlg .
  DATA i_set TYPE zhdbsqls .
  DATA i_set_friname1 TYPE filename_al11 .
  DATA i_set_friname TYPE filename_al11 .
  DATA i_rootfolder TYPE filename_al11 .
  DATA i_usersstore TYPE char151 .
  DATA i_hdbsql_script TYPE tty_string .
  DATA i_select_script TYPE tty_string .
  DATA i_ttimestamp TYPE char1 .
  DATA i_timestamp TYPE char14 .
  DATA i_hdbsql_script_loc TYPE tty_string .
  DATA i_keepcsv TYPE char1 .
  DATA i_dbschema TYPE char20 .
  DATA i_ttname TYPE char250_d .
  DATA i_ddef_script TYPE tty_string .
  DATA i_select_script_tt TYPE tty_string .
  DATA i_max_reqtsn TYPE rspm_request_tsn .
  DATA i_alert_only TYPE char1 .
  DATA i_archive_util TYPE ty_util.

  METHODS set_hdbsql_script .
  METHODS set_select_script .
  METHODS set_hdbsql_script_loc .
  METHODS get_delta
    RETURNING
      VALUE(o_res) TYPE zhdbsqldelta .
  METHODS set_delta
    IMPORTING
      !in_delta TYPE zhdbsqldelta .
  METHODS set_select_script_tt .
  METHODS get_delta_text
    RETURNING
      VALUE(o_res) TYPE char80 .
  METHODS get_maxreqtsn
    IMPORTING
      !i_adso TYPE rsoadsonm
    RETURNING
      VALUE(o_res) TYPE rspm_request_tsn .
PRIVATE SECTION.

  DATA i_pos_cnt TYPE int2 .
  DATA i_pos_current TYPE zhdbsqlpos .

  METHODS get_folder
    RETURNING
      VALUE(o_res) TYPE string .
  METHODS drop_tt .
  METHODS set_ddef_script .
  METHODS get_count
    RETURNING
      VALUE(o_res) TYPE int2 .
  METHODS get_1stpos
    RETURNING
      VALUE(o_res) TYPE zhdbsqlpos .
ENDCLASS.



CLASS ZCL_BW_HDBSQLCSV_BASE IMPLEMENTATION.


  METHOD constructor.
    me->i_setgroup = i_setgroup.
    me->i_set = i_set.

    SELECT g, s, pos FROM zhdbsqlgs
      INTO TABLE @DATA(lt_t)
     WHERE g = @me->i_setgroup
       AND s = @me->i_set
       AND status = 'A'.
    i_pos_cnt = lines( lt_t ).
    i_pos_current = 1.
    GET TIME STAMP FIELD DATA(lv_ts).
    CONVERT TIME STAMP lv_ts TIME ZONE sy-zonlo INTO DATE DATA(dat) TIME DATA(tim).

    IF i_set_friname = ''.
      me->i_set_friname = i_set.
      me->i_set_friname1 = i_set.
    ELSE.
      me->i_set_friname = i_set_friname.
      me->i_set_friname1 = i_set_friname.
    ENDIF.
    CONDENSE me->i_set_friname NO-GAPS.
    CONDENSE me->i_set_friname1 NO-GAPS.
" Лучше добавить в имя файла. Там же и connection менять на английский, если надо
*    me->i_set_friname = |{ me->i_set_friname }_ru|.
*    me->i_set_friname1 = |{ me->i_set_friname1 }_ru|.

    " @ is placeholder to be replaced later by position (char1)
    me->i_set_friname = |{ me->i_set_friname }_@|.
    me->i_set_friname1 = |{ me->i_set_friname1 }_@|.
    i_ttname = to_upper( |#{ me->i_set_friname }_{ me->i_setgroup }_{ me->i_set }| ).

    me->i_rootfolder = i_rootfolder.
    me->i_usersstore = i_usersstore.
    me->i_ttimestamp = i_ttimestamp.
    me->i_keepcsv = i_keepcsv.

    me->i_timestamp = ''.
    IF me->i_ttimestamp = '1'. " date only
      me->i_timestamp = |_{ dat }|.
    ELSEIF me->i_ttimestamp = '2'. " datetime
      me->i_timestamp = |_{ dat }_{ tim }|.
      "e->i_timestamp = |{ dat+4(4) }{ dat+3(2) }{ dat+0(2) }{ tim }|.
    ENDIF.
    me->i_set_friname = |{ me->i_set_friname }{ me->i_timestamp }|.

    CALL FUNCTION 'DB_DBSCHEMA_CURRENT'
      IMPORTING
        dbschema = i_dbschema.

    i_archive_util = zip.
    " i_ttname = substring( val = to_upper( |#{ i_set_friname }_{ me->i_setgroup }_{ me->i_set }| ) off = 1 len = 120 ).
  ENDMETHOD.


  METHOD get_folder.
    o_res = |{ me->i_rootfolder }/{ me->i_setgroup }/{ me->i_set }|.
  ENDMETHOD.


  METHOD make_hdbsql_script.
    DATA: file      TYPE string.

    me->set_hdbsql_script( ).
    DATA(lv_fn1) =  me->i_set_friname1.
    REPLACE FIRST OCCURRENCE OF '@' IN lv_fn1 WITH me->i_pos_current IN CHARACTER MODE.

    file = |{ me->get_folder( ) }/hdbsql_{ lv_fn1 }.sh|.
    TRY.
        OPEN DATASET file FOR OUTPUT IN TEXT MODE ENCODING DEFAULT WITH SMART LINEFEED.
        IF sy-subrc <> 0.
          WRITE: / |Dataset for file { file } has errors while opening|.
        ENDIF.
        LOOP AT me->i_hdbsql_script ASSIGNING FIELD-SYMBOL(<fs>).
          TRANSFER <fs>-row TO file.
        ENDLOOP.
        CLOSE DATASET file.
        "WRITE: / | File created: { file }|.
      CATCH cx_root INTO DATA(lx_root).
        DATA(lv_err) = lx_root->kernel_errid.
        WRITE: / | Error creating { file }: { lv_err }|.
    ENDTRY.
  ENDMETHOD.


  METHOD make_select_script.
    DATA: file TYPE string.

    DATA(lv_fn) =  me->i_set_friname.
    REPLACE FIRST OCCURRENCE OF '@' IN lv_fn WITH me->i_pos_current IN CHARACTER MODE.

    me->set_select_script( ).
    file = |{ me->get_folder( ) }/arch/{ lv_fn }.sql|.
    TRY.
        OPEN DATASET file FOR OUTPUT IN TEXT MODE ENCODING DEFAULT WITH SMART LINEFEED.
        LOOP AT me->i_select_script ASSIGNING FIELD-SYMBOL(<fs>).
          TRANSFER <fs>-row TO file.
        ENDLOOP.
        CLOSE DATASET file.
        "WRITE: / | File created: { file }|.
      CATCH cx_root INTO DATA(lx_root).
        DATA(lv_err) = lx_root->kernel_errid.
        WRITE: / | Error creating { file }: { lv_err }|.
    ENDTRY.
  ENDMETHOD.


  METHOD set_hdbsql_script.
    DATA: lv_row    TYPE ty_string.

    CLEAR: me->i_hdbsql_script.
    " lv_row-row = |/usr/sap/hdbclient/hdbuserstore LIST > /home/bwdadm/HDBOUT.txt|.
    " APPEND lv_row TO me->i_hdbsql_script.
    " execute core hdbsql-script

    DATA(lv_fn1) =  me->i_set_friname1. " without date[time] run
    REPLACE FIRST OCCURRENCE OF '@' IN lv_fn1 WITH me->i_pos_current IN CHARACTER MODE.
*    DATA(lv_fdef) =  me->i_set_friname1. " without date[time] run
*    REPLACE FIRST OCCURRENCE OF '_@' IN lv_fdef WITH '' IN CHARACTER MODE.
    DATA(lv_fn) =  me->i_set_friname. " with date[time] run
    REPLACE FIRST OCCURRENCE OF '@' IN lv_fn  WITH me->i_pos_current IN CHARACTER MODE.

    lv_row-row = |hdbsql -U { me->i_usersstore } -I { me->get_folder( ) }/arch/{ lv_fn }.sql -a -f -C -x -resultencoding UTF8 |.
    lv_row-row = |{ lv_row-row }-o { me->get_folder( ) }/{ lv_fn }{ me->get_delta_text(  ) }.csv|.
    lv_row-row = |{ lv_row-row } >{ me->get_folder( ) }/errorlog_{ me->i_pos_current }.txt 2>&1|.
    APPEND lv_row TO me->i_hdbsql_script.

    " zipping
    lv_row-row = |if grep -q ":" errorlog_{ me->i_pos_current }.txt;|.
    APPEND lv_row TO me->i_hdbsql_script.
    lv_row-row = |then|.
    APPEND lv_row TO me->i_hdbsql_script.
    lv_row-row = | echo "Error while running hdbsql"|.
    APPEND lv_row TO me->i_hdbsql_script.
    lv_row-row = |else|.
    APPEND lv_row TO me->i_hdbsql_script.

    IF me->i_alert_only <> 'X'.
      DATA(lv_zip_file) = |{ lv_fn }{ me->get_delta_text( ) }|.
    ELSE.
      lv_zip_file = |alert|.
    ENDIF.

    IF i_archive_util = me->zip.

        lv_row-row = | zip -9 -j { me->get_folder( ) }/arch/{ lv_zip_file }.zip | &
                     | { me->get_folder( ) }/{ lv_fn }{ me->get_delta_text(  ) }.csv |.
        APPEND lv_row TO me->i_hdbsql_script.
        lv_row-row = | zip -9 -j { me->get_folder( ) }/arch/{ lv_zip_file }.zip | &
                     | { me->get_folder( ) }/{ lv_fn1 }.def|.
        APPEND lv_row TO me->i_hdbsql_script.
        lv_row-row = | mv { me->get_folder( ) }/arch/{ lv_zip_file }.zip { me->get_folder( ) }/zip|.
        APPEND lv_row TO me->i_hdbsql_script.

        IF me->i_keepcsv = ''.
            " removing source csv-file
            lv_row-row = | rm -e { me->get_folder( ) }/{ lv_fn }{ me->get_delta_text(  ) }.csv|.
            APPEND lv_row TO me->i_hdbsql_script.
            lv_row-row = | rm -e { me->get_folder( ) }/{ lv_fn1 }.def|.
            APPEND lv_row TO me->i_hdbsql_script.
            " self-deletion of sh after execution. Put # for single line comment
            lv_row-row = | rm -e { me->get_folder( ) }/hdbsql_{ lv_fn1 }.sh|.
            APPEND lv_row TO me->i_hdbsql_script.
        ENDIF.

    ELSEIF i_archive_util = me->gzip.

        IF me->i_keepcsv = 'X'.
          DATA(lv_keep) = '-k'.
        ENDIF.

        lv_row-row = | gzip -9 { lv_keep } { me->get_folder( ) }/{ lv_fn }{ me->get_delta_text(  ) }.csv |.
        APPEND lv_row TO me->i_hdbsql_script.
        lv_row-row = | mv { me->get_folder( ) }/{ lv_zip_file }.csv.gz { me->get_folder( ) }/zip/{ lv_zip_file }.gz|.
        APPEND lv_row TO me->i_hdbsql_script.
        lv_row-row = | mv { me->get_folder( ) }/{ lv_fn1 }.def { me->get_folder( ) }/zip/{ lv_zip_file }.def|.
        APPEND lv_row TO me->i_hdbsql_script.

    ENDIF.

    lv_row-row = | chmod o+rw { me->get_folder( ) }/zip/{ lv_zip_file }.{ CONV char4( me->i_archive_util ) }|.
    APPEND lv_row TO me->i_hdbsql_script.
    lv_row-row = |fi|.
    APPEND lv_row TO me->i_hdbsql_script.

  ENDMETHOD.


  METHOD set_select_script.
    "abstract. must be overrided


    DATA: lv_low TYPE ty_string.
  ENDMETHOD.


  METHOD drop_tt.
    DATA:
      lo_conn       TYPE REF TO cl_sql_connection,
      lo_statement  TYPE REF TO cl_sql_statement,
      lo_result_set TYPE REF TO cl_sql_result_set,
      lx_sql        TYPE REF TO cx_sql_exception.

    DATA(lv_tt) = me->i_ttname.
    REPLACE FIRST OCCURRENCE OF '@' IN lv_tt WITH me->i_pos_current IN CHARACTER MODE.

    DATA(lv_stmt) = |DROP TABLE { lv_tt };|.
    TRY.
        lo_conn = cl_sql_connection=>get_connection( ).
        lo_statement = lo_conn->create_statement( ).
        DATA(l_row_cnt) = lo_statement->execute_update( lv_stmt ).
        "WRITE: / |Local temporary table { me->i_ttname } was deleted|.
      CATCH cx_sql_exception INTO lx_sql.
*        WRITE: / lv_stmt, ' : ', lx_sql->get_text( ).
*        WRITE: / lx_sql->sql_code.
*        WRITE: / lx_sql->sql_message.
    ENDTRY.
  ENDMETHOD.


  METHOD get_delta.
    SELECT SINGLE delta INTO o_res
      FROM zhdbsqlgs
     WHERE g = i_setgroup
       AND s = i_set
       AND pos = i_pos_current.
  ENDMETHOD.


  METHOD get_delta_text.
  ENDMETHOD.


  METHOD make_ddef_script.
    DATA: file      TYPE string.

    DATA(lv_fn1) =  me->i_set_friname1.
    REPLACE FIRST OCCURRENCE OF '@' IN lv_fn1 WITH me->i_pos_current IN CHARACTER MODE.

    file = |{ me->get_folder( ) }/{ lv_fn1 }.def|.
    me->set_ddef_script( ).
    TRY.
        OPEN DATASET file FOR OUTPUT IN TEXT MODE ENCODING DEFAULT WITH SMART LINEFEED.
        IF sy-subrc <> 0.
          WRITE: / |Dataset for file { file } has errors while opening|.
        ENDIF.
        LOOP AT me->i_ddef_script ASSIGNING FIELD-SYMBOL(<fs>).
          TRANSFER <fs>-row TO file.
        ENDLOOP.
        CLOSE DATASET file.
        "WRITE: / | File created: { file }|.
      CATCH cx_root INTO DATA(lx_root).
        DATA(lv_err) = lx_root->kernel_errid.
        WRITE: / | Error creating { file }: { lv_err }|.
    ENDTRY.
  ENDMETHOD.


  METHOD make_ddef_script_loc.
      DATA: lv_filepath(40) TYPE c.
    lv_filepath = 'C:\Temp\'.

    DATA(lv_fn1) =  me->i_set_friname1.
    REPLACE FIRST OCCURRENCE OF '@' IN lv_fn1 WITH me->i_pos_current IN CHARACTER MODE.

    DATA(lv_filename) = |{ lv_filepath }{ lv_fn1 }.def|.
    me->set_ddef_script( ).
    CALL FUNCTION 'GUI_DOWNLOAD'
      EXPORTING
*       BIN_FILESIZE          =
        filename              = lv_filename
        filetype              = 'ASC'
        append                = ' '
        write_field_separator = ';'
*       HEADER                = '00'
*       TRUNC_TRAILING_BLANKS = ' '
*       WRITE_LF              = 'X'
*       COL_SELECT            = ' '
*       COL_SELECT_MASK       = ' '
        dat_mode              = 'X'
*       CONFIRM_OVERWRITE     = ' '
*       NO_AUTH_CHECK         = ' '
*       CODEPAGE              = ' '
*       IGNORE_CERR           = ABAP_TRUE
*       REPLACEMENT           = '#'
*       WRITE_BOM             = ' '
*       TRUNC_TRAILING_BLANKS_EOL       = 'X'
*       WK1_N_FORMAT          = ' '
*       WK1_N_SIZE            = ' '
*       WK1_T_FORMAT          = ' '
*       WK1_T_SIZE            = ' '
*       WRITE_LF_AFTER_LAST_LINE        = ABAP_TRUE
*       SHOW_TRANSFER_STATUS  = ABAP_TRUE
*       VIRUS_SCAN_PROFILE    = '/SCET/GUI_DOWNLOAD'
*     IMPORTING
*       FILELENGTH            =
      TABLES
        data_tab              = me->i_ddef_script
"       fieldnames            =
*     EXCEPTIONS
*       FILE_WRITE_ERROR      = 1
*       NO_BATCH              = 2
*       GUI_REFUSE_FILETRANSFER         = 3
*       INVALID_TYPE          = 4
*       NO_AUTHORITY          = 5
*       UNKNOWN_ERROR         = 6
*       HEADER_NOT_ALLOWED    = 7
*       SEPARATOR_NOT_ALLOWED = 8
*       FILESIZE_NOT_ALLOWED  = 9
*       HEADER_TOO_LONG       = 10
*       DP_ERROR_CREATE       = 11
*       DP_ERROR_SEND         = 12
*       DP_ERROR_WRITE        = 13
*       UNKNOWN_DP_ERROR      = 14
*       ACCESS_DENIED         = 15
*       DP_OUT_OF_MEMORY      = 16
*       DISK_FULL             = 17
*       DP_TIMEOUT            = 18
*       FILE_NOT_FOUND        = 19
*       DATAPROVIDER_EXCEPTION          = 20
*       CONTROL_FLUSH_ERROR   = 21
*       OTHERS                = 22
      .
    IF sy-subrc <> 0.
      WRITE: / | Error saving file { lv_filename }|.
    ELSE.
      WRITE: / | File created: { lv_filename } |.
    ENDIF.
  ENDMETHOD.


  METHOD make_hdbsql_script_loc.
    DATA: lv_filepath(40) TYPE c.
    lv_filepath = 'C:\Temp\'.

    DATA(lv_fn1) =  me->i_set_friname1.
    REPLACE FIRST OCCURRENCE OF '@' IN lv_fn1 WITH me->i_pos_current IN CHARACTER MODE.

    DATA(lv_filename) = |{ lv_filepath }hdbsql_{ lv_fn1 }.bat|.
    me->set_hdbsql_script_loc( ).
    CALL FUNCTION 'GUI_DOWNLOAD'
      EXPORTING
*       BIN_FILESIZE          =
        filename              = lv_filename
        filetype              = 'ASC'
        append                = ' '
        write_field_separator = ';'
*       HEADER                = '00'
*       TRUNC_TRAILING_BLANKS = ' '
*       WRITE_LF              = 'X'
*       COL_SELECT            = ' '
*       COL_SELECT_MASK       = ' '
        dat_mode              = 'X'
*       CONFIRM_OVERWRITE     = ' '
*       NO_AUTH_CHECK         = ' '
*       CODEPAGE              = ' '
*       IGNORE_CERR           = ABAP_TRUE
*       REPLACEMENT           = '#'
*       WRITE_BOM             = ' '
*       TRUNC_TRAILING_BLANKS_EOL       = 'X'
*       WK1_N_FORMAT          = ' '
*       WK1_N_SIZE            = ' '
*       WK1_T_FORMAT          = ' '
*       WK1_T_SIZE            = ' '
*       WRITE_LF_AFTER_LAST_LINE        = ABAP_TRUE
*       SHOW_TRANSFER_STATUS  = ABAP_TRUE
*       VIRUS_SCAN_PROFILE    = '/SCET/GUI_DOWNLOAD'
*     IMPORTING
*       FILELENGTH            =
      TABLES
        data_tab              = me->i_hdbsql_script_loc
"       fieldnames            =
*     EXCEPTIONS
*       FILE_WRITE_ERROR      = 1
*       NO_BATCH              = 2
*       GUI_REFUSE_FILETRANSFER         = 3
*       INVALID_TYPE          = 4
*       NO_AUTHORITY          = 5
*       UNKNOWN_ERROR         = 6
*       HEADER_NOT_ALLOWED    = 7
*       SEPARATOR_NOT_ALLOWED = 8
*       FILESIZE_NOT_ALLOWED  = 9
*       HEADER_TOO_LONG       = 10
*       DP_ERROR_CREATE       = 11
*       DP_ERROR_SEND         = 12
*       DP_ERROR_WRITE        = 13
*       UNKNOWN_DP_ERROR      = 14
*       ACCESS_DENIED         = 15
*       DP_OUT_OF_MEMORY      = 16
*       DISK_FULL             = 17
*       DP_TIMEOUT            = 18
*       FILE_NOT_FOUND        = 19
*       DATAPROVIDER_EXCEPTION          = 20
*       CONTROL_FLUSH_ERROR   = 21
*       OTHERS                = 22
      .
    IF sy-subrc <> 0.
      WRITE: / | Error saving file { lv_filename }|.
    ELSE.
      WRITE: / | File created: { lv_filename } |.
    ENDIF.
  ENDMETHOD.


  METHOD make_select_script_loc.
      DATA: lv_filepath(40) TYPE c.
    lv_filepath = 'C:\Temp\'.

    DATA(lv_fn) =  me->i_set_friname.
    REPLACE FIRST OCCURRENCE OF '@' IN lv_fn WITH me->i_pos_current IN CHARACTER MODE.

    DATA(lv_filename) = |{ lv_filepath }{ lv_fn }.sql|.
    me->set_select_script( ).
    CALL FUNCTION 'GUI_DOWNLOAD'
      EXPORTING
*       BIN_FILESIZE          =
        filename              = lv_filename
        filetype              = 'ASC'
        append                = ' '
        write_field_separator = ';'
*       HEADER                = '00'
*       TRUNC_TRAILING_BLANKS = ' '
*       WRITE_LF              = 'X'
*       COL_SELECT            = ' '
*       COL_SELECT_MASK       = ' '
        dat_mode              = 'X'
*       CONFIRM_OVERWRITE     = ' '
*       NO_AUTH_CHECK         = ' '
*       CODEPAGE              = ' '
*       IGNORE_CERR           = ABAP_TRUE
*       REPLACEMENT           = '#'
*       WRITE_BOM             = ' '
*       TRUNC_TRAILING_BLANKS_EOL       = 'X'
*       WK1_N_FORMAT          = ' '
*       WK1_N_SIZE            = ' '
*       WK1_T_FORMAT          = ' '
*       WK1_T_SIZE            = ' '
*       WRITE_LF_AFTER_LAST_LINE        = ABAP_TRUE
*       SHOW_TRANSFER_STATUS  = ABAP_TRUE
*       VIRUS_SCAN_PROFILE    = '/SCET/GUI_DOWNLOAD'
*     IMPORTING
*       FILELENGTH            =
      TABLES
        data_tab              = me->i_select_script
"       fieldnames            =
*     EXCEPTIONS
*       FILE_WRITE_ERROR      = 1
*       NO_BATCH              = 2
*       GUI_REFUSE_FILETRANSFER         = 3
*       INVALID_TYPE          = 4
*       NO_AUTHORITY          = 5
*       UNKNOWN_ERROR         = 6
*       HEADER_NOT_ALLOWED    = 7
*       SEPARATOR_NOT_ALLOWED = 8
*       FILESIZE_NOT_ALLOWED  = 9
*       HEADER_TOO_LONG       = 10
*       DP_ERROR_CREATE       = 11
*       DP_ERROR_SEND         = 12
*       DP_ERROR_WRITE        = 13
*       UNKNOWN_DP_ERROR      = 14
*       ACCESS_DENIED         = 15
*       DP_OUT_OF_MEMORY      = 16
*       DISK_FULL             = 17
*       DP_TIMEOUT            = 18
*       FILE_NOT_FOUND        = 19
*       DATAPROVIDER_EXCEPTION          = 20
*       CONTROL_FLUSH_ERROR   = 21
*       OTHERS                = 22
      .
    IF sy-subrc <> 0.
      WRITE: / | Error saving file: { lv_filename }|.
    ELSE.
      WRITE: / | File created: { lv_filename }|.
    ENDIF.
  ENDMETHOD.


  METHOD print_scripts.
    WRITE: / |***** hdbsql.sh *****|.
    LOOP AT me->i_hdbsql_script ASSIGNING FIELD-SYMBOL(<fsh>).
      WRITE:  /  <fsh>-row.
    ENDLOOP.
    WRITE: / |***** hdbsql.bat *****|.
    LOOP AT me->i_hdbsql_script_loc ASSIGNING <fsh>.
      WRITE:  /  <fsh>-row.
    ENDLOOP.
    WRITE: / |***** select.sql *****|.
    LOOP AT me->i_select_script ASSIGNING <fsh>.
      WRITE:  /  <fsh>-row.
    ENDLOOP.
    WRITE: / |***** data definition *****|.
    LOOP AT me->i_ddef_script ASSIGNING <fsh>.
      WRITE:  /  <fsh>-row.
    ENDLOOP.
    WRITE: /.
  ENDMETHOD.


  METHOD set_ddef_script.
    TYPES:
      BEGIN OF t_ddef,
        column_name    TYPE char80,
        data_type_name TYPE char80,
        length         TYPE int8,
        scale          TYPE int8,
      END OF t_ddef,
      tt_ddef TYPE STANDARD TABLE OF t_ddef.

    DATA:
      lo_conn       TYPE REF TO cl_sql_connection,
      lo_statement  TYPE REF TO cl_sql_statement,
      lo_result_set TYPE REF TO cl_sql_result_set,
      lx_sql        TYPE REF TO cx_sql_exception,
      lt_res        TYPE REF TO data,
      lt_table      TYPE tt_ddef,
      lv_row        TYPE ty_string.

    me->set_select_script_tt( ).
    me->drop_tt( ).
    CLEAR: me->i_ddef_script.

    DATA(lv_tt) = me->i_ttname.
    REPLACE FIRST OCCURRENCE OF '@' IN lv_tt WITH me->i_pos_current IN CHARACTER MODE.

    DATA(lv_stmt) = |CREATE LOCAL TEMPORARY TABLE  { lv_tt } AS (|.
    LOOP AT me->i_select_script_tt ASSIGNING FIELD-SYMBOL(<fs>).
      lv_stmt = |{ lv_stmt }{ <fs>-row } |.
    ENDLOOP.
    lv_stmt = |{ lv_stmt } );|.
    TRY.
        lo_conn = cl_sql_connection=>get_connection( ).
        lo_statement = lo_conn->create_statement( ).
        DATA(l_row_cnt) = lo_statement->execute_update( lv_stmt ).
        "WRITE: / |Local temporary table { me->i_ttname } was created|.
        GET REFERENCE OF lt_table INTO lt_res.
        lv_stmt = | SELECT column_name, data_type_name, length, scale FROM M_TEMPORARY_TABLE_COLUMNS | &&
                  |  WHERE table_name = '{ lv_tt }' AND schema_name = '{ me->i_dbschema }'| &&
                  |  ORDER BY position ASC; |.
        DATA(l_res_ref) = lo_statement->execute_query( lv_stmt ).
        l_res_ref->set_param_table( lt_res ).
        l_res_ref->next_package( ).
        l_res_ref->close( ).
        IF lt_table IS NOT INITIAL.
          LOOP AT lt_table ASSIGNING FIELD-SYMBOL(<fs_table>).
            lv_row-row = |{ <fs_table>-column_name },{ <fs_table>-data_type_name }| &&
                         |,{ <fs_table>-length },{ <fs_table>-scale }|.
            APPEND lv_row TO me->i_ddef_script.
          ENDLOOP.
        ENDIF.
      CATCH cx_sql_exception INTO lx_sql.
        WRITE: / lv_stmt, ' : ', lx_sql->get_text( ).
        WRITE: / lx_sql->sql_code.
        WRITE: / lx_sql->sql_message.
    ENDTRY.
    me->drop_tt( ).
  ENDMETHOD.


  METHOD set_delta.
    SELECT SINGLE * INTO @DATA(ls_gs) FROM zhdbsqlgs
     WHERE g = @i_setgroup
       AND s = @i_set
       AND pos = @i_pos_current.
    IF sy-dbcnt = 0.
      WRITE: / |ERROR: No record in table zhdbsqlgs for { i_setgroup }-{ i_set }-{ i_pos_current }.|.
    ELSE.
      ls_gs-delta = in_delta.
      UPDATE zhdbsqlgs FROM @ls_gs.
*    IF sy-dbcnt = 0.
*      INSERT INTO zra_pchain_log VALUES ls_pchain_log.
*      IF sy-dbcnt NE 1.
*        WRITE: / |Ошибка добавления записей в zra_pchain_log: { in_pproc }:{ in_date }|.
*      ENDIF.
*    ENDIF.
    ENDIF.
  ENDMETHOD.


  METHOD set_hdbsql_script_loc.
    DATA: lv_row    TYPE ty_string.

    DATA(lv_fn) =  me->i_set_friname.
    REPLACE FIRST OCCURRENCE OF '@' IN lv_fn WITH me->i_pos_current IN CHARACTER MODE.

    CLEAR: me->i_hdbsql_script_loc.
    lv_row-row = |hdbsql -U { me->i_usersstore } -I "{ lv_fn }.sql" -a -f -C -x -resultencoding UTF8 |.
    lv_row-row = |{ lv_row-row }-o "{ lv_fn }.csv"|.
    APPEND lv_row TO me->i_hdbsql_script_loc.
    lv_row-row = |pause|.
    APPEND lv_row TO me->i_hdbsql_script_loc.

  ENDMETHOD.


  METHOD set_select_script_tt.
    " need to be overrided

  ENDMETHOD.


  METHOD get_count.
    SELECT g, s, pos FROM zhdbsqlgs
      INTO TABLE @DATA(lt_t)
     WHERE g = @me->i_setgroup
       AND s = @me->i_set
       AND status = 'A'.
    o_res = lines( lt_t ).
  ENDMETHOD.


  METHOD make_primary_script.
    DATA(lv_count) = me->get_count( ).
    DATA: lt_script TYPE tty_string.
    DATA: lv_row    TYPE ty_string.
    DATA: file      TYPE string.

    IF lv_count = 1.
      "me->i_pos_current = '1'.
      me->i_pos_current = me->get_1stpos( ).
      me->make_select_script( ).
      me->make_hdbsql_script( ).
      me->make_ddef_script( ).
      DATA(lv_fn1) =  me->i_set_friname1.
      REPLACE FIRST OCCURRENCE OF '@' IN lv_fn1 WITH me->i_pos_current IN CHARACTER MODE.
      lv_row-row = |sh { me->get_folder( ) }/hdbsql_{ lv_fn1 }.sh|. " No & at the end!!
      APPEND lv_row TO lt_script.
    ELSEIF lv_count > 1.
      SELECT * INTO @DATA(wa)
        FROM zhdbsqlgs
       WHERE g = @me->i_setgroup
         AND s = @me->i_set
         AND status = 'A'
        ORDER BY pos ASCENDING.
        me->i_pos_current = wa-pos.
        me->make_select_script( ).
        me->make_hdbsql_script( ).
        me->make_ddef_script( ).

        lv_fn1 =  me->i_set_friname1.
        REPLACE FIRST OCCURRENCE OF '@' IN lv_fn1 WITH me->i_pos_current IN CHARACTER MODE.
        lv_row-row = |sh { me->get_folder( ) }/hdbsql_{ lv_fn1 }.sh &|.
        APPEND lv_row TO lt_script.
      ENDSELECT.
    ELSE.
      WRITE: / |No Active positions defined in ZHDBSQLGS for g={ me->i_setgroup } s={ me->i_set }.|.
      RETURN.
    ENDIF.

    file = |{ me->get_folder( ) }/script.sh|.
    lv_row-row = |rm -e { file }|.
    APPEND lv_row TO lt_script.
    TRY.
        OPEN DATASET file FOR OUTPUT IN TEXT MODE ENCODING DEFAULT WITH SMART LINEFEED.
        IF sy-subrc <> 0.
          WRITE: / |Dataset for file { file } has errors while opening|.
        ENDIF.
        LOOP AT lt_script ASSIGNING FIELD-SYMBOL(<fs>).
          TRANSFER <fs>-row TO file.
        ENDLOOP.
        CLOSE DATASET file.
        "WRITE: / | File created: { file }|.
      CATCH cx_root INTO DATA(lx_root).
        DATA(lv_err) = lx_root->kernel_errid.
        WRITE: / | Error creating { file }: { lv_err }|.
    ENDTRY.
  ENDMETHOD.


  METHOD make_primary_script_loc.
    DATA(lv_count) = me->get_count( ).
    IF lv_count = 1.
      "me->i_pos_current = '1'.
      me->i_pos_current = me->get_1stpos( ).
      me->make_select_script_loc( ).
      me->make_hdbsql_script_loc( ).
      me->make_ddef_script_loc( ).
    ELSEIF lv_count > 1.
      "WRITE: / |No active positions to create hdbsql script|.
      SELECT * INTO @DATA(wa)
        FROM zhdbsqlgs
       WHERE g = @me->i_setgroup
         AND s = @me->i_set
         AND status = 'A'
        ORDER BY pos ASCENDING.
        me->i_pos_current = wa-pos.
        me->make_select_script_loc( ).
        me->make_hdbsql_script_loc( ).
        me->make_ddef_script_loc( ).
      ENDSELECT.
    ELSE.
      WRITE: / |No Active positions defined in ZHDBSQLGS for g={ me->i_setgroup } s={ me->i_set }.|.
      RETURN.
    ENDIF.
  ENDMETHOD.


  METHOD after_run_hdbsql.
      " May be override in child classes.
  ENDMETHOD.


  METHOD before_run_hdbsql.
    " May be override in child classes.
  ENDMETHOD.


  METHOD get_maxreqtsn.

    TYPES:
      BEGIN OF t_reqtsn,
        reqtsn TYPE rspm_request_tsn,
      END OF t_reqtsn,
      tt_reqtsn TYPE STANDARD TABLE OF t_reqtsn.

    DATA:
      lo_conn       TYPE REF TO cl_sql_connection,
      lo_statement  TYPE REF TO cl_sql_statement,
      lo_result_set TYPE REF TO cl_sql_result_set,
      lx_sql        TYPE REF TO cx_sql_exception,
      lt_res        TYPE REF TO data,
      lt_table      TYPE tt_reqtsn,
      lv_stmt       TYPE string,
      lv_row        TYPE ty_string.

    TRY.
      o_res = '00000000000000000000000'.
        lo_conn = cl_sql_connection=>get_connection( ).
        lo_statement = lo_conn->create_statement( ).
        GET REFERENCE OF lt_table INTO lt_res.

        lv_stmt = |select max( request_tsn ) from  | &
       |  ( select coalesce( request_tsn, '00000000000000000000000' ) as request_tsn | &
       |       from ( select max( request_tsn ) as request_tsn | &
       |       from rspmrequest where tlogo = 'ADSO'and datatarget = '{ i_adso }' and storage = 'AQ'and request_status in ( 'GG', 'GR' ) | &
       |        and request_tsn < ( select min( request_tsn ) | &
       |         from rspmrequest where tlogo = 'ADSO' | &
       |          and datatarget = '{ i_adso }' | &
       |          and storage = 'AQ' | &
       |          and request_status not in ( 'GG', 'GR', 'D', 'M' ) ) ) | &
       |         union select coalesce( request_tsn, '00000000000000000000000' ) as request_tsn | &
       |                 from ( select max( request_tsn ) as request_tsn from rspmrequest | &
       |                         where tlogo = 'ADSO'and datatarget = '{ i_adso }' | &
       |                          and storage = 'AQ'and request_status in ( 'GG', 'GR' ) | &
       |                          and not exists ( select '' from rspmrequest | &
       |                                            where tlogo = 'ADSO'and datatarget = '{ i_adso }' | &
       |                                              and storage = 'AQ'and request_status not in ( 'GG', 'GR', 'D', 'M' ) ) ) ) |.

        DATA(l_res_ref) = lo_statement->execute_query( lv_stmt ).
        l_res_ref->set_param_table( lt_res ).
        l_res_ref->next_package( ).
        l_res_ref->close( ).
        IF lt_table IS NOT INITIAL.
          LOOP AT lt_table ASSIGNING FIELD-SYMBOL(<fs_table>).
            IF <fs_table>-reqtsn IS NOT INITIAL.
              o_res = <fs_table>-reqtsn.
            ENDIF.
            RETURN.
          ENDLOOP.
        ENDIF.
      CATCH cx_sql_exception INTO lx_sql.
        WRITE: / lv_stmt, ' : ', lx_sql->get_text( ).
        WRITE: / lx_sql->sql_code.
        WRITE: / lx_sql->sql_message.
    ENDTRY.

  ENDMETHOD.


  METHOD get_1stpos.
    " must be called only if number of positions equal 1
    o_res = ''.
    SELECT SINGLE pos INTO @o_res FROM zhdbsqlgs
     WHERE g = @me->i_setgroup
       AND s = @me->i_set
       AND status = 'A'.

  ENDMETHOD.


  METHOD make_log.
    DATA: l_uuid_x16 TYPE sysuuid_x16.
    TRY.
        SELECT g, s, pos, description, delta INTO TABLE @DATA(lt_gs) FROM zhdbsqlgs
         WHERE g = @i_setgroup
           AND s = @i_set
           AND status = 'A'.
        IF sy-dbcnt = 0.
          WRITE: / |ERROR: No record in table zhdbsqlgs for { i_setgroup }-{ i_set }.|.
        ELSE.
          l_uuid_x16 = cl_system_uuid=>create_uuid_x16_static( ).
          SELECT * INTO TABLE @DATA(lt_log)  FROM zhdbsqllog WHERE g = 'XXXX'.
          SELECT SINGLE * INTO @DATA(ls_log) FROM zhdbsqllog WHERE g = 'XXXX'.
          GET TIME STAMP FIELD DATA(lv_ts).
          CONVERT TIME STAMP lv_ts TIME ZONE sy-zonlo INTO DATE DATA(dat) TIME DATA(tim).
          LOOP AT lt_gs ASSIGNING FIELD-SYMBOL(<fs_gs>).
            ls_log-guidid = l_uuid_x16.
            ls_log-runts = |{ dat }{ tim }|.
            MOVE-CORRESPONDING <fs_gs> TO ls_log.
            APPEND ls_log TO lt_log.
          ENDLOOP.
          INSERT zhdbsqllog FROM TABLE @lt_log.
        ENDIF.
      CATCH cx_uuid_error INTO DATA(e_text).
        MESSAGE e_text->get_text( ) TYPE 'I'.
    ENDTRY.
  ENDMETHOD.


  METHOD update_missed.
    " must be overrided in super class
  ENDMETHOD.
ENDCLASS.