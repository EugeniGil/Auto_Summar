#PREPROCESSING INFO FROM ONLINE COURSES

#import packages
import requests
import cx_Oracle
import numpy as np
import pandas as pd
import os
import datetime

from dotenv import load_dotenv

#database connection 

#use it to load variables from environment
load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASSWORD= os.getenv('DB_PASSWORD')
DB_DNS = os.getenv('DB_DNS')



#QUERY 1
c = conn.cursor()
query = c.execute('''
select a.*,  
 to_char(to_date(mod(a.suma1,86400),'sssss'),'hh24:mi:ss') duracion,  
 to_char(to_date(fecha_final, 'DD-MM-YYYY') - to_date(fecha_inicio, 'DD-MM-YYYY')) fechas
 from (  
 select  
    sum(results.tiempo1) as suma1,  
    results.pk,  
     'O' || results.pk pk2,   
    results.anio,  
    results.usuario,  
    results.curso,  
      substr(to_char(numtodsinterval(sum(SUBSTR(results.tiempo, 1, 2)*3600 + SUBSTR(results.tiempo, 4, 2)*60 + SUBSTR(results.tiempo, 7, 2)), 'SECOND')),1,19) suma,  
    trunc(sysdate - min(results.fecha1)) tiempo_curso,  
   to_char(min(results.fecha1), 'dd/MM/yyyy') as fecha_inicio,  
    to_char(max(results.fecha1),'dd/MM/yyyy') as fecha_final  
 from  
    (  
   SELECT DISTINCT  
     DATOS.USERID || DATOS.COURSEID as PK,  
     DATOS.COMPONENT as component,  
           DATOS.FECHA as fecha1,  
           EXTRACT(year from datos.fecha) Anio,  
           DATOS.USERID usuario,  
           DATOS.COURSEID curso,  
          CASE WHEN SUM((DATOS.fecha_dos - DATOS.fecha_uno))  = 0  
                THEN to_char(trunc(sysdate) + SUM((DATOS.fecha_dos_A - DATOS.fecha_uno_A)) + SUM(DATOS.DURACIONGRABACION) / 86400, 'hh24:mi:ss')  
         ELSE   to_char(trunc(sysdate) + SUM((DATOS.fecha_dos_b - DATOS.fecha_uno_b)) + SUM(DATOS.DURACIONGRABACION) / 86400 , 'hh24:mi:ss') END  TiempoEstimado,  
         to_char(trunc(sysdate) + SUM((DATOS.fecha_dos_B - DATOS.fecha_uno_B)) , 'hh24:mi:ss') Tiempo,  
         to_char(trunc(sysdate) + SUM((DATOS.fecha_dos_B - DATOS.fecha_uno_B)) , 'sssss') Tiempo1,  
         to_char(trunc(sysdate) + SUM(DATOS.DURACIONGRABACION) / 86400, 'hh24:mi:ss') as Grabaciones  
    FROM  
    (  
           select  
             l.component, 
             l.eventname,
             trunc(l.timecreated) as Fecha,  
             l.userid,  
             l.courseid,  
             min(l.timecreated) over (PARTITION BY l.userid,trunc(l.timecreated),l.courseid,l.contextlevel,l.component order by l.timecreated)  FECHA_UNO,  
             max(l.timecreated) over (PARTITION BY l.userid,trunc(l.timecreated),l.courseid,l.contextlevel,l.component order by l.timecreated) FECHA_DOS,  
             min(l.timecreated) over (PARTITION BY l.userid,trunc(l.timecreated),l.courseid order by l.timecreated)  FECHA_UNO_A,  
             max(l.timecreated) over (PARTITION BY l.userid,trunc(l.timecreated),l.courseid order by l.timecreated) FECHA_DOS_A,  
             nvl(lag(l.timecreated) over (PARTITION BY l.userid,trunc(l.timecreated),l.courseid order by l.timecreated),l.timecreated) FECHA_UNO_B,  
             nvl(lead(l.timecreated) over (PARTITION BY l.userid,trunc(l.timecreated),l.courseid order by l.timecreated),l.timecreated) FECHA_DOS_B,  
             l.edulevel,  
             l.contextlevel,  
             CASE WHEN l.component ='mod_collaborate' and  
                 (  
                    (l.action = 'viewed' and l.target='recording')  
                    or  
                    (l.action = 'launched' and l.target ='session')  
                ) THEN  l.objectid  
                 WHEN L.COMPONENT ='plagiarism_unicheck' THEN L.OBJECTID  
                 ELSE 0 END objectid,  
             nvl(c.Duration,0) as DuracionGrabacion,  
            row_number() over (PARTITION BY l.userid,trunc(l.timecreated),l.courseid,l.contextlevel,l.component,  
                                    CASE WHEN l.component ='mod_collaborate' and  
                                                    (  
                                                        (l.action = 'viewed' and l.target='recording')  
                                                        or  
                                                        (l.action = 'launched' and l.target ='session')  
                                                    ) THEN  l.objectid  
                                         WHEN L.COMPONENT ='plagiarism_unicheck' THEN L.OBJECTID  
                                    ELSE 0 END order by l.timecreated desc) rn  
            ,l.objectid      
         from stage2.mdl_logstore_standard_log l  
             left join stage2.mdl_collaborate c on l.objectid = c.id and l.component ='mod_collaborate'  
             and ((l.action = 'viewed' and l.target='recording')  
              or (l.action = 'launched' and l.target ='session') )
        WHERE trunc(L.timecreated) =  trunc(sysdate)-1
      order by l.timecreated       
  ) DATOS  
    WHERE  DATOS.COMPONENT not in ('core', 'tool_certificate') 
    AND DATOS.EVENTNAME <> '\mod_scorm\event\course_module_viewed'
     GROUP BY DATOS.FECHA,  
          DATOS.USERID,  
          DATOS.COURSEID,  
		  DATOS.COMPONENT  
     ORDER BY 2,4  
     ) RESULTS  
    group by   results.pk,  
    results.anio, 
    results.usuario,  
    results.curso  
    order by results.pk  
    ) a  
			   where a.curso not in (163, 106,111,112,113,114,115,116,117)   
                '''
                 )

#fecth query 
rows = c.fetchall()
#format df
rows_df = pd.DataFrame(rows)

#preprocessing 
def preprocessing(df):
    if df.empty == False:
        df = df.rename({0: 'nada', 1 : 'pk', 2: 'pk2', 3:'anio', 4: 'usuario', 5:'curso', 
                         6: 'suma', 7:'tiempo_curso', 8:'fecha_ini', 9: 'fecha_fin', 10: 'duracion',
                          11:'fechas', 12:'asistencia_real', 13: 'asistencia_teorica'}, axis = 'columns')
        df = df.drop(['nada', 'suma', 'tiempo_curso', 'fechas'], axis = 'columns')
    else:
        df = pd.DataFrame(columns = ['pk','pk2','anio', 'usuario','curso', 
                          'fecha_ini', 'fecha_fin',  'duracion',
                           'asistencia_real', 'asistencia_teorica'])
    return df

rows_df = preprocessing(rows_df)

#traigo la segunda query 
cur = conn.cursor()
cur.execute('''
select distinct  
    b.id || c.id as PK,  
     case when upper(substr(extra.modalidad,1,1)) <> 'P' then  'O'|| b.id || c.id    
     else 'P'|| b.id || c.id   
     end   
     pk2,  
  cat.name as vc_objetivo,  
    a.cod_curso_moodle as codigo_curso,  
     c.fullname as nombre_curso,  
    extra.modalidad,  
    extra.anio as cursacad,  
    b.id as username,  
    l.cod_asistente as idnumber,  
    a.cod_accion_form as AAFF,  
   HORAS.DURACION , 
  GRUPO.GRUPO  
from stage2.mdl_uax_listaasistentesaccform l   
join stage2.mdl_uax_listaaccionformativa a   
on l.cod_accion_form = a.cod_accion_form    
join stage2.mdl_course c   
on a.cod_curso_moodle = c.shortname   
inner join stage2.mdl_user b on b.idnumber = l.cod_asistente   
inner join stage2.mdl_uax_curso_extra extra on extra.shortname= c.shortname  
LEFT JOIN STAGE2.MDL_COURSE_CATEGORIES CAT ON C.CATEGORY = CAT.ID  
INNER JOIN stage2.mdl_context con ON con.instanceid = c.id  
LEFT JOIN (   
 select    
      A.CONTEXTID,A.CHARVALUE AS DURACION   
  from   
    stage2.mdl_customfield_data A   
   inner join stage2.MDL_CUSTOMFIELD_FIELD  b on a.fieldid = b.id    
   WHERE  B.id IN (7)   
   ) HORAS   
 ON HORAS.CONTEXTID = CON.ID  
 left join (  
 select   
      A.CONTEXTID,A.CHARVALUE AS GRUPO   
 from   
   stage2.mdl_customfield_data A   
   inner join stage2.MDL_CUSTOMFIELD_FIELD  b on a.fieldid = b.id   
   WHERE  B.id IN (11)  
   ) GRUPO   
 ON GRUPO.CONTEXTID = CON.ID  
 
'''
                 )
#fecth la query
datos = cur.fetchall()
#format
datos_df = pd.DataFrame(datos)
datos_df = datos_df.rename({0 : 'pk', 1: 'pk2', 3:'codigo_curso', 4: 'nombre_curso', 5:'modalidad', 6:'curso', 
                            7:'usuario' , 8:'idnumber', 9: 'aaff'}, axis = 'columns')
datos_df = datos_df.drop([2,10,11], axis = 'columns')


#merge queries
df = rows_df.merge(datos_df, how = 'left', on = 'pk')
#eliminamos posibles duplicados
df = df.drop_duplicates()
#dejamos solo la teleformación para evitar problemas
df = df[df['modalidad']== 'Teleformación']

#query status
cur = conn.cursor()
cur.execute('''
select distinct  
 gg.USERID || c.id as PK,  
 cursos.anio,  
 c.id,  
 gg.userid,  
 to_char(gg.finalgrade) finalgrade  
 from stage2.mdl_course c    
     left join stage2.mdl_uax_curso_extra cursos  on c.shortname = cursos.shortname  
     left JOIN stage2.mdl_course_categories  cc ON cc.id = c.category     
     left JOIN stage2.mdl_context  ctx ON c.id = ctx.instanceid     
     left JOIN stage2.mdl_role_assignments  ra ON ra.contextid = ctx.id     
     left JOIN stage2.mdl_user  u ON u.id = ra.userid     
     left JOIN stage2.mdl_grade_items  gi ON gi.courseid = c.id     
     LEFT JOIN stage2.mdl_grade_grades  gg ON gi.id = gg.itemid AND  gg.userid = u.id   
    
'''
                 )
#fecth query 
status = cur.fetchall()

#arreglos a status que siempre viene con información 
status_df = pd.DataFrame(status)
status_df = status_df.drop([1, 2, 3], axis = 'columns')
status_df = status_df.rename({0:'pk', 4:'notafinal'}, axis = 'columns')
status_df['notafinal'] = status_df['notafinal'].str.replace(',','.')
status_df['notafinal'] = pd.to_numeric(status_df['notafinal'])
df = df.rename({'pk_x': 'pk'}, axis = 'columns')

#2nd merge 
df = df.merge(status_df, how = 'left', on = 'pk')
#sacamos duplicados 

def situacion_asi(df):
    if df.empty == False:
        if (df['notafinal'] < 50) | (pd.isna(df['notafinal'])):
            return 2 
        elif(df['notafinal'] >= 50) :
            return 3
        else :
            return 2
    else:
        pass

def transforma_2(df):
    if df.empty == False:
        df['situacion'] = df.apply(situacion_asi, axis = 1)
    else:
        pass
    return df


df= transforma_2(df)

def definir(df):
    if df.empty == False:
        dataframe = df[['idnumber', 'aaff', 'fecha_ini', 'duracion', 'situacion']]
    else:
        dataframe = df.copy()
    return dataframe

dataframe = definir(df)

def eliminar_0(df):
    if df.empty == False:
        df = df[df['duracion'] != '00:00:00']
    else:
        df =  pd.DataFrame(columns = [['idnumber', 'aaff', 'fecha_ini', 'duracion', 'situacion']])
    return df

dataframe = eliminar_0(dataframe)

#ajustes de strings para que el endpoint lo entienda
#funcion para pasarlo por todas
def preparacion_xml_online(df):
	if df.empty == False:
		df = df.dropna(subset = 'fecha_ini')
		df['duracion'] = df['duracion'].str[:5]
		df['duracion'] = df['duracion'].str.replace(':','.')
		df['duracion'] = df['duracion'].str.replace('00.00','00.01')
		df['fecha_ini'] = df['fecha_ini'].str.replace('/','-')
		df['fecha_ini'] = pd.to_datetime(df['fecha_ini'], infer_datetime_format=True).dt.date
	else:
		pass
	return df

dataframe = preparacion_xml_online(dataframe)


dataframe = dataframe[(dataframe['aaff'] == '2201-01' ) | (dataframe['aaff']== '2201-02') | (dataframe['aaff']== '2202-01') | (dataframe['aaff']== '2202-02') | (dataframe['aaff']== '2203-01') | (dataframe['aaff']== '2203-02')]















