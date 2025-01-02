"утилиты для заполнения фильтров в DTP и инфо-пакетах
class ZCLBW_ZHR_DATE_UTILS definition
  public
  final
  create public .

public section.

  types:
    ty_range type standard table of rssdlrange with default key .

  methods FILL_DTP_SELECTION_CALDAY
    importing
      !I_T_RANGE type TY_RANGE
    returning
      value(E_T_RANGE) type TY_RANGE .
  methods CONSTRUCTOR
    importing
      !I_CALMONTH type RSCALMONTH default SY-DATUM(6)
      !I_SHIFT_IN_MONTHS type I default 0
      !I_SHIFT_IN_MONTHS_LOW type I default 0
      !I_FIELDNAME type STRING default 'CALMON' .
  methods GET_CALMONTH_SHIFTED
    importing
      !I_SHIFT_IN_MONTHS type I
    returning
      value(E_CALMONTH) type RSCALMONTH .
  methods FILL_DTP_SELECTION_CALMONTH
    importing
      !I_T_RANGE type TY_RANGE
    returning
      value(E_T_RANGE) type TY_RANGE .
  protected section.
private section.

  data:
    c_fieldname         type c length 30 .
  data CALMONTH type RSCALMONTH .
  data SHIFT_IN_MONTHS_LOW type I .
  data SHIFT_IN_MONTHS type I .
ENDCLASS.



CLASS ZCLBW_ZHR_DATE_UTILS IMPLEMENTATION.


  method constructor.
    calmonth = i_calmonth.
    c_fieldname = i_fieldname.
    shift_in_months = i_shift_in_months.
    shift_in_months_low = i_shift_in_months_low.
  endmethod.


  method FILL_DTP_SELECTION_CALDAY.
    DATA: lv_date type /BI0/OICALDAY.
    e_t_range[] = i_t_range[].
    assign e_t_range[ fieldname = c_fieldname ] to field-symbol(<fs_range>).
    if sy-subrc = 0.
      CONCATENATE calmonth '01' INTO lv_date.
      CALL FUNCTION 'UJD_ADD_MONTH_TO_DATE'
        EXPORTING
          I_MONTHS = shift_in_months
          I_OLD_DATE = lv_date
        IMPORTING
          E_NEW_DATE = lv_date
          .
      <fs_range>-sign = 'I'.
      <fs_range>-option = 'BT'.
      CONCATENATE lv_date(6) '01' INTO <fs_range>-low.
      CALL FUNCTION 'SN_LAST_DAY_OF_MONTH'
        EXPORTING
          DAY_IN = lv_date
        IMPORTING
          END_OF_MONTH = lv_date
          .
      <fs_range>-high = lv_date.
    endif.
  endmethod.


  method fill_dtp_selection_calmonth.
    e_t_range[] = i_t_range[].
    assign e_t_range[ fieldname = c_fieldname ] to field-symbol(<fs_range>).
    if sy-subrc = 0.
      <fs_range>-sign = 'I'.
      if shift_in_months = shift_in_months_low.
        <fs_range>-option = 'EQ'.
        <fs_range>-low = me->get_calmonth_shifted( shift_in_months ).
        clear <fs_range>-high.
      else.
        <fs_range>-option = 'BT'.
        <fs_range>-low = me->get_calmonth_shifted( shift_in_months_low ).
        <fs_range>-high = me->get_calmonth_shifted( shift_in_months ).
      endif.
    endif.
  endmethod.


  method get_calmonth_shifted.
    data(calmon_date) = conv d( calmonth && '01').
    call function 'SEPA_MANDATE_ADD_MONTH_TO_DATE'
      exporting
        months  = i_shift_in_months
        olddate = calmon_date
      importing
        newdate = calmon_date.
    e_calmonth = calmon_date(6).
  endmethod.
ENDCLASS.