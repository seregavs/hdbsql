@EndUserText.label : 'Метаданные групп и наборов для выгрузки через hdbsql'
@AbapCatalog.enhancement.category : #NOT_EXTENSIBLE
@AbapCatalog.tableCategory : #TRANSPARENT
@AbapCatalog.deliveryClass : #C
@AbapCatalog.dataMaintenance : #ALLOWED
define table zhdbsqlgs {
  key mandt   : mandt not null;
  key g       : zhdbsqlg not null;
  key s       : zhdbsqls not null;
  key pos     : zhdbsqlpos not null;
  status      : zhdbsqlstatus not null;
  description : zhdbsqldescr;
  delta       : zhdbsqldelta;

}

@EndUserText.label : 'Журнал выгрузки через hdbsql'
@AbapCatalog.enhancement.category : #NOT_EXTENSIBLE
@AbapCatalog.tableCategory : #TRANSPARENT
@AbapCatalog.deliveryClass : #A
@AbapCatalog.dataMaintenance : #DISPLAY
define table zhdbsqllog {
  key mandt   : mandt not null;
  key guidid  : char32 not null;
  key pos     : zhdbsqlpos not null;
  runts       : char14 not null;
  g           : zhdbsqlg not null;
  s           : zhdbsqls not null;
  description : zhdbsqldescr;
  delta       : zhdbsqldelta;

}