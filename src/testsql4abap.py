# Вспомогательный код для генерации ABAP-кода метода с SELECT-запросом для hdbsql
# Каждой строке с SQL-script-кодом (или SQL-SELECT-выражением) в файле C:\Temp\hdbsql\\anonymous.sql генерируется строка для ABAP
# Результат сохраняется в anonymous_abap.txt, откуда copy-paste переносится в ABAP-редактор
# Без выполнения этого py-кода пришлось бы писать ABAP-выражения APPEND ... вручную

mainlist = []
infile = open('C:\Temp\hdbsql\\anonymous.sql','r') 
# infile = open('C:\Temp\hdbsql\createtable.sql','r') 
for line in infile:
    mainlist.append(str(line[:-1])
                    .rstrip()
                    .replace('\t', '')  
                    .replace('SAPBWP', "{ me->i_dbschema }")
                    .replace("\\", "\\\\")
                    )
infile.close()

s_output = ''
mainlist = [line for line in mainlist if str(line).lstrip()[:2] != '--']
for index, line in enumerate(mainlist):
    mainlist[index] = r'lv_row-row = |{0} |.'.format(line.replace('|', '\|')) + '\nAPPEND lv_row TO me->i_select_script.'
    s_output += '{0}\n'.format(mainlist[index])

print(mainlist)

with open("C:\Temp\hdbsql\\anonymous_abap.txt", "w") as f:
# with open("C:\Temp\hdbsql\create_table_abap.txt","w") as f:
    f.write(s_output)