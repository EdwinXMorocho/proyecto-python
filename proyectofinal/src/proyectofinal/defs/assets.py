import dagster as dg
import pandas as pd
import requests
from datetime import datetime, timedelta
import numpy as np
from typing import Dict, Any
import os

# Configuración
COVID_URLS = [
    "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv",
    "https://catalog.ourworldindata.org/garden/covid/latest/compact/compact.csv",
    "https://covid.ourworldindata.org/data/owid-covid-data.csv"
]
PAIS_COMPARATIVO = "Colombia"

# PASO 1: LECTURA DE DATOS

@dg.asset(
    description="Lee datos de COVID-19 desde OWID sin transformar",
    automation_condition=dg.AutomationCondition.on_cron("0 6 * * *")  # Diario a las 6 AM
)
def leer_datos(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """
    Obtiene el CSV de COVID-19 desde URLs de OWID.
    Prueba múltiples URLs de respaldo.
    No aplica limpieza ni filtros.
    """
    
    for i, url in enumerate(COVID_URLS):
        try:
            context.log.info(f"Intentando descargar desde URL {i+1}: {url}")
            response = requests.get(url, timeout=120)
            response.raise_for_status()
            
            # Guardar temporalmente para cargar con pandas
            temp_path = "/tmp/covid_raw.csv"
            with open(temp_path, 'wb') as f:
                f.write(response.content)
            
            df = pd.read_csv(temp_path)
            context.log.info(f"¡Éxito! Datos cargados desde URL {i+1}: {len(df)} filas, {len(df.columns)} columnas")
            
            # DIAGNÓSTICO: Ver qué columnas tenemos realmente
            context.log.info(f"Columnas disponibles: {list(df.columns)}")
            context.log.info("Primeras 5 filas:")
            for idx, row in df.head().iterrows():
                context.log.info(f"  {idx}: {dict(row)}")
            
            # Metadata para Dagster UI
            context.add_output_metadata({
                'url_exitoso': dg.MetadataValue.text(url),
                'filas': dg.MetadataValue.int(len(df)),
                'columnas': dg.MetadataValue.int(len(df.columns)),
                'columnas_disponibles': dg.MetadataValue.text(", ".join(df.columns)),
                'rango_fechas': dg.MetadataValue.text(f"{df['date'].min()} a {df['date'].max()}" if 'date' in df.columns else "Sin columna date"),
                'preview': dg.MetadataValue.md(df.head().to_markdown())
            })
            
            return df
            
        except requests.exceptions.RequestException as e:
            context.log.warning(f"Error con URL {i+1} ({url}): {str(e)}")
            if i == len(COVID_URLS) - 1:  # Si es el último URL
                context.log.error("Todos los URLs fallaron")
                raise
            continue
        except Exception as e:
            context.log.error(f"Error inesperado con URL {i+1}: {e}")
            if i == len(COVID_URLS) - 1:
                raise
            continue
    
    raise Exception("No se pudo descargar datos desde ningún URL")

# PASO 2: CHEQUEOS DE ENTRADA (Como assets normales)

@dg.asset(
    description="Reporte de chequeos de entrada de datos",
    deps=[leer_datos]
)
def chequeos_entrada(context: dg.AssetExecutionContext, leer_datos: pd.DataFrame) -> pd.DataFrame:
    """Ejecuta todos los chequeos de entrada y genera reporte"""
    df = leer_datos.copy()
    resultados = []
    
    # DIAGNÓSTICO: Mostrar columnas disponibles
    context.log.info(f"Columnas disponibles para chequeos: {list(df.columns)}")
    
    # Check 1: Fechas futuras (si existe columna de fecha)
    fecha_cols = [col for col in df.columns if 'date' in col.lower()]
    if fecha_cols:
        fecha_col = fecha_cols[0]  # Usar la primera columna de fecha encontrada
        df[fecha_col] = pd.to_datetime(df[fecha_col])
        fecha_maxima = df[fecha_col].max()
        hoy = pd.Timestamp.now().normalize()
        fechas_futuras = len(df[df[fecha_col] > hoy])
        
        resultados.append({
            'regla': 'fechas_no_futuras',
            'estado': 'PASS' if fechas_futuras == 0 else 'FAIL',
            'filas_afectadas': fechas_futuras,
            'notas': f"Fecha máxima: {fecha_maxima.date()}, columna usada: {fecha_col}"
        })
    else:
        resultados.append({
            'regla': 'fechas_no_futuras',
            'estado': 'SKIP',
            'filas_afectadas': 0,
            'notas': "No se encontró columna de fecha"
        })
    
    # Check 2: Columnas clave (buscar equivalentes)
    location_cols = [col for col in df.columns if any(word in col.lower() for word in ['location', 'country', 'entity'])]
    population_cols = [col for col in df.columns if 'population' in col.lower()]
    
    problemas_columnas = 0
    notas_columnas = []
    
    if not location_cols:
        problemas_columnas += 1
        notas_columnas.append("No se encontró columna de ubicación/país")
    else:
        location_col = location_cols[0]
        nulos = df[location_col].isnull().sum()
        if nulos > 0:
            problemas_columnas += nulos
            notas_columnas.append(f"{location_col}: {nulos} nulos")
    
    if not population_cols:
        notas_columnas.append("No se encontró columna de población")
    else:
        pop_col = population_cols[0]
        nulos = df[pop_col].isnull().sum()
        if nulos > 0:
            problemas_columnas += nulos
            notas_columnas.append(f"{pop_col}: {nulos} nulos")
    
    resultados.append({
        'regla': 'columnas_clave_validas',
        'estado': 'PASS' if problemas_columnas == 0 else 'WARN',
        'filas_afectadas': problemas_columnas,
        'notas': "; ".join(notas_columnas) if notas_columnas else f"OK - ubicación: {location_cols}, población: {population_cols}"
    })
    
    # Check 3: Duplicados (si tenemos las columnas necesarias)
    if location_cols and fecha_cols:
        duplicados = df.duplicated(subset=[location_cols[0], fecha_cols[0]]).sum()
        resultados.append({
            'regla': 'unicidad_location_date',
            'estado': 'PASS' if duplicados == 0 else 'WARN',
            'filas_afectadas': duplicados,
            'notas': f"Duplicados encontrados: {duplicados}"
        })
    else:
        resultados.append({
            'regla': 'unicidad_location_date',
            'estado': 'SKIP',
            'filas_afectadas': 0,
            'notas': "No se pudieron verificar duplicados - columnas faltantes"
        })
    
    # Check 4: Población válida
    if population_cols:
        pop_col = population_cols[0]
        pop_invalida = (df[pop_col] <= 0).sum()
        resultados.append({
            'regla': 'poblacion_positiva',
            'estado': 'PASS' if pop_invalida == 0 else 'WARN',
            'filas_afectadas': pop_invalida,
            'notas': f"Poblaciones <= 0: {pop_invalida}"
        })
    else:
        resultados.append({
            'regla': 'poblacion_positiva',
            'estado': 'SKIP',
            'filas_afectadas': 0,
            'notas': "No se encontró columna de población"
        })
    
    df_resultado = pd.DataFrame(resultados)
    
    context.add_output_metadata({
        'total_reglas': dg.MetadataValue.int(len(resultados)),
        'reglas_pass': dg.MetadataValue.int(len(df_resultado[df_resultado['estado'] == 'PASS'])),
        'reglas_warn': dg.MetadataValue.int(len(df_resultado[df_resultado['estado'] == 'WARN'])),
        'reglas_fail': dg.MetadataValue.int(len(df_resultado[df_resultado['estado'] == 'FAIL'])),
        'reglas_skip': dg.MetadataValue.int(len(df_resultado[df_resultado['estado'] == 'SKIP'])),
        'reporte': dg.MetadataValue.md(df_resultado.to_markdown())
    })
    
    context.log.info("Chequeos de entrada completados")
    for _, row in df_resultado.iterrows():
        context.log.info(f"  {row['regla']}: {row['estado']} ({row['filas_afectadas']} filas)")
    
    return df_resultado

# PASO 3: PROCESAMIENTO DE DATOS  

@dg.asset(
    description="Procesa y filtra datos para Ecuador y país comparativo",
    deps=[leer_datos, chequeos_entrada]  # Depende de chequeos
)
def datos_procesados(context: dg.AssetExecutionContext, leer_datos: pd.DataFrame, chequeos_entrada: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y filtra datos para Ecuador y país comparativo.
    Elimina nulos y duplicados, selecciona columnas esenciales.
    """
    df = leer_datos.copy()
    
    context.log.info(f"Datos iniciales: {len(df)} filas")
    context.log.info(f"Columnas disponibles: {list(df.columns)}")
    
    # Detectar nombres de columnas automáticamente
    location_cols = [col for col in df.columns if any(word in col.lower() for word in ['location', 'country', 'entity'])]
    date_cols = [col for col in df.columns if 'date' in col.lower()]
    cases_cols = [col for col in df.columns if any(word in col.lower() for word in ['new_cases', 'cases', 'daily_cases'])]
    vacc_cols = [col for col in df.columns if any(word in col.lower() for word in ['people_vaccinated', 'vaccinated', 'vaccination'])]
    pop_cols = [col for col in df.columns if 'population' in col.lower()]
    
    context.log.info(f"Columnas detectadas - Location: {location_cols}, Date: {date_cols}, Cases: {cases_cols}, Vaccinated: {vacc_cols}, Population: {pop_cols}")
    
    if not location_cols or not date_cols:
        raise ValueError(f"No se encontraron columnas esenciales. Location: {location_cols}, Date: {date_cols}")
    
    # Usar las primeras columnas encontradas
    location_col = location_cols[0]
    date_col = date_cols[0]
    
    # Convertir date a datetime
    df[date_col] = pd.to_datetime(df[date_col])
    
    # Filtrar por países de interés
    paises_interes = ['Ecuador', PAIS_COMPARATIVO]
    df_filtrado = df[df[location_col].isin(paises_interes)].copy()
    context.log.info(f"Después de filtrar países {paises_interes}: {len(df_filtrado)} filas")
    
    # Seleccionar columnas esenciales (las que existan)
    columnas_seleccionar = [location_col, date_col]
    
    if cases_cols:
        columnas_seleccionar.append(cases_cols[0])
    if vacc_cols:
        columnas_seleccionar.append(vacc_cols[0])  
    if pop_cols:
        columnas_seleccionar.append(pop_cols[0])
        
    context.log.info(f"Columnas a seleccionar: {columnas_seleccionar}")
    df_filtrado = df_filtrado[columnas_seleccionar].copy()
    
    # Renombrar columnas para uso estándar
    rename_dict = {location_col: 'location', date_col: 'date'}
    if cases_cols:
        rename_dict[cases_cols[0]] = 'new_cases'
    if vacc_cols:
        rename_dict[vacc_cols[0]] = 'people_vaccinated'
    if pop_cols:
        rename_dict[pop_cols[0]] = 'population'
        
    df_filtrado = df_filtrado.rename(columns=rename_dict)
    
    # Eliminar filas con valores nulos en columnas clave (solo si existen)
    filas_antes = len(df_filtrado)
    
    columnas_a_limpiar = []
    if 'new_cases' in df_filtrado.columns:
        columnas_a_limpiar.append('new_cases')
    if 'people_vaccinated' in df_filtrado.columns:
        columnas_a_limpiar.append('people_vaccinated')
        
    if columnas_a_limpiar:
        df_filtrado = df_filtrado.dropna(subset=columnas_a_limpiar)
        filas_despues = len(df_filtrado)
        context.log.info(f"Eliminadas {filas_antes - filas_despues} filas con nulos en {columnas_a_limpiar}")
    
    # Eliminar duplicados
    duplicados_antes = df_filtrado.duplicated(subset=['location', 'date']).sum()
    df_filtrado = df_filtrado.drop_duplicates(subset=['location', 'date'])
    context.log.info(f"Eliminados {duplicados_antes} duplicados")
    
    # Ordenar por país y fecha
    df_filtrado = df_filtrado.sort_values(['location', 'date'])
    
    context.add_output_metadata({
        'filas_finales': dg.MetadataValue.int(len(df_filtrado)),
        'paises': dg.MetadataValue.text(f"Ecuador, {PAIS_COMPARATIVO}"),
        'columnas_finales': dg.MetadataValue.text(f"{list(df_filtrado.columns)}"),
        'rango_fechas': dg.MetadataValue.text(f"{df_filtrado['date'].min().date()} a {df_filtrado['date'].max().date()}"),
        'preview': dg.MetadataValue.md(df_filtrado.head(10).to_markdown())
    })
    
    return df_filtrado

# PASO 4: CÁLCULO DE MÉTRICAS

@dg.asset(
    description="Calcula incidencia acumulada a 7 días por 100k habitantes",
    deps=[datos_procesados]
)
def metrica_incidencia_7d(context: dg.AssetExecutionContext, datos_procesados: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula incidencia diaria por 100k habitantes y promedio móvil de 7 días.
    """
    df = datos_procesados.copy()
    
    # Calcular incidencia diaria por 100k habitantes
    df['incidencia_diaria'] = (df['new_cases'] / df['population']) * 100000
    
    # Calcular promedio móvil de 7 días por país
    resultados = []
    
    for pais in df['location'].unique():
        df_pais = df[df['location'] == pais].copy()
        df_pais = df_pais.sort_values('date')
        
        # Promedio móvil de 7 días
        df_pais['incidencia_7d'] = df_pais['incidencia_diaria'].rolling(
            window=7, min_periods=1
        ).mean()
        
        # Seleccionar solo las columnas necesarias
        df_resultado = df_pais[['date', 'location', 'incidencia_7d']].copy()
        df_resultado.columns = ['fecha', 'pais', 'incidencia_7d']
        
        resultados.append(df_resultado)
    
    resultado_final = pd.concat(resultados, ignore_index=True)
    
    context.add_output_metadata({
        'filas': dg.MetadataValue.int(len(resultado_final)),
        'incidencia_max': dg.MetadataValue.float(float(resultado_final['incidencia_7d'].max())),
        'incidencia_promedio': dg.MetadataValue.float(float(resultado_final['incidencia_7d'].mean())),
        'preview': dg.MetadataValue.md(resultado_final.tail(10).to_markdown())
    })
    
    return resultado_final

@dg.asset(
    description="Calcula factor de crecimiento semanal de casos",
    deps=[datos_procesados]
)
def metrica_factor_crec_7d(context: dg.AssetExecutionContext, datos_procesados: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula factor de crecimiento semanal:
    casos_semana_actual / casos_semana_anterior
    """
    df = datos_procesados.copy()
    resultados = []
    
    for pais in df['location'].unique():
        df_pais = df[df['location'] == pais].copy()
        df_pais = df_pais.sort_values('date')
        
        # Calcular sumas móviles de 7 días
        df_pais['casos_semana_actual'] = df_pais['new_cases'].rolling(
            window=7, min_periods=7
        ).sum()
        
        df_pais['casos_semana_prev'] = df_pais['new_cases'].rolling(
            window=7, min_periods=7
        ).sum().shift(7)
        
        # Calcular factor de crecimiento
        df_pais['factor_crec_7d'] = df_pais['casos_semana_actual'] / df_pais['casos_semana_prev']
        
        # Filtrar solo filas con datos completos
        df_pais = df_pais.dropna(subset=['factor_crec_7d'])
        
        # Preparar resultado
        df_resultado = df_pais[['date', 'location', 'casos_semana_actual', 'factor_crec_7d']].copy()
        df_resultado.columns = ['semana_fin', 'pais', 'casos_semana', 'factor_crec_7d']
        
        resultados.append(df_resultado)
    
    if resultados:
        resultado_final = pd.concat(resultados, ignore_index=True)
        
        context.add_output_metadata({
            'filas': dg.MetadataValue.int(len(resultado_final)),
            'factor_max': dg.MetadataValue.float(float(resultado_final['factor_crec_7d'].max())),
            'factor_promedio': dg.MetadataValue.float(float(resultado_final['factor_crec_7d'].mean())),
            'preview': dg.MetadataValue.md(resultado_final.tail(10).to_markdown())
        })
        
        return resultado_final
    else:
        # Retornar DataFrame vacío si no hay datos
        return pd.DataFrame(columns=['semana_fin', 'pais', 'casos_semana', 'factor_crec_7d'])

# PASO 5: CHEQUEOS DE SALIDA (Como assets normales)

@dg.asset(
    description="Reporte de chequeos de salida de métricas",
    deps=[metrica_incidencia_7d, metrica_factor_crec_7d]
)
def chequeos_salida(context: dg.AssetExecutionContext, metrica_incidencia_7d: pd.DataFrame, metrica_factor_crec_7d: pd.DataFrame) -> pd.DataFrame:
    """Ejecuta chequeos de salida sobre las métricas calculadas"""
    resultados = []
    
    # Check 1: Rango de incidencia
    if not metrica_incidencia_7d.empty:
        fuera_rango = metrica_incidencia_7d[(metrica_incidencia_7d['incidencia_7d'] < 0) | 
                                           (metrica_incidencia_7d['incidencia_7d'] > 2000)]
        
        resultados.append({
            'regla': 'incidencia_7d_rango_valido',
            'estado': 'PASS' if len(fuera_rango) == 0 else 'FAIL',
            'filas_afectadas': len(fuera_rango),
            'notas': f"Rango [0, 2000]. Min: {float(metrica_incidencia_7d['incidencia_7d'].min()):.2f}, Max: {float(metrica_incidencia_7d['incidencia_7d'].max()):.2f}"
        })
    
    # Check 2: Factor de crecimiento válido
    if not metrica_factor_crec_7d.empty:
        problematicos = metrica_factor_crec_7d[
            (metrica_factor_crec_7d['factor_crec_7d'] <= 0) | 
            (metrica_factor_crec_7d['factor_crec_7d'].isna()) | 
            (np.isinf(metrica_factor_crec_7d['factor_crec_7d']))
        ]
        
        resultados.append({
            'regla': 'factor_crecimiento_valido',
            'estado': 'PASS' if len(problematicos) == 0 else 'WARN',
            'filas_afectadas': len(problematicos),
            'notas': f"Min: {float(metrica_factor_crec_7d['factor_crec_7d'].min()):.2f}, Max: {float(metrica_factor_crec_7d['factor_crec_7d'].max()):.2f}"
        })
    
    df_resultado = pd.DataFrame(resultados)
    
    context.add_output_metadata({
        'total_reglas': dg.MetadataValue.int(len(resultados)),
        'reglas_pass': dg.MetadataValue.int(len(df_resultado[df_resultado['estado'] == 'PASS'])),
        'reglas_warn': dg.MetadataValue.int(len(df_resultado[df_resultado['estado'] == 'WARN'])),
        'reglas_fail': dg.MetadataValue.int(len(df_resultado[df_resultado['estado'] == 'FAIL'])),
        'reporte': dg.MetadataValue.md(df_resultado.to_markdown())
    })
    
    return df_resultado

# PASO 6: EXPORTACIÓN DE RESULTADOS

@dg.asset(
    description="Genera reporte Excel con datos procesados y métricas",
    deps=[datos_procesados, metrica_incidencia_7d, metrica_factor_crec_7d, chequeos_entrada, chequeos_salida]
)
def reporte_excel_covid(
    context: dg.AssetExecutionContext,
    datos_procesados: pd.DataFrame,
    metrica_incidencia_7d: pd.DataFrame,
    metrica_factor_crec_7d: pd.DataFrame,
    chequeos_entrada: pd.DataFrame,
    chequeos_salida: pd.DataFrame
) -> str:
    """
    Exporta resultados finales a archivo Excel con múltiples hojas.
    """
    
    # Crear directorio si no existe
    output_dir = "/workspaces/proyecto-python/reportes"
    os.makedirs(output_dir, exist_ok=True)
    
    # Definir ruta de salida
    output_path = f"{output_dir}/reporte_covid_ecuador.xlsx"
    
    try:
        # Crear archivo Excel con múltiples hojas
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Hoja 1: Datos procesados
            datos_procesados.to_excel(writer, sheet_name='Datos_Procesados', index=False)
            
            # Hoja 2: Métrica incidencia
            metrica_incidencia_7d.to_excel(writer, sheet_name='Incidencia_7d', index=False)
            
            # Hoja 3: Métrica factor crecimiento
            metrica_factor_crec_7d.to_excel(writer, sheet_name='Factor_Crecimiento_7d', index=False)
            
            # Hoja 4: Chequeos de entrada
            chequeos_entrada.to_excel(writer, sheet_name='Chequeos_Entrada', index=False)
            
            # Hoja 5: Chequeos de salida
            chequeos_salida.to_excel(writer, sheet_name='Chequeos_Salida', index=False)
            
            # Hoja 6: Resumen estadístico
            resumen = pd.DataFrame({
                'Métrica': ['Datos Procesados', 'Incidencia 7d', 'Factor Crecimiento 7d'],
                'Filas': [len(datos_procesados), len(metrica_incidencia_7d), len(metrica_factor_crec_7d)],
                'Fecha_Min': [
                    datos_procesados['date'].min() if not datos_procesados.empty else 'N/A',
                    metrica_incidencia_7d['fecha'].min() if not metrica_incidencia_7d.empty else 'N/A', 
                    metrica_factor_crec_7d['semana_fin'].min() if not metrica_factor_crec_7d.empty else 'N/A'
                ],
                'Fecha_Max': [
                    datos_procesados['date'].max() if not datos_procesados.empty else 'N/A',
                    metrica_incidencia_7d['fecha'].max() if not metrica_incidencia_7d.empty else 'N/A',
                    metrica_factor_crec_7d['semana_fin'].max() if not metrica_factor_crec_7d.empty else 'N/A'
                ]
            })
            resumen.to_excel(writer, sheet_name='Resumen', index=False)
        
        context.log.info(f"Reporte Excel generado: {output_path}")
        
        # También guardar CSVs individuales
        csv_dir = f"{output_dir}/csv_reports"
        os.makedirs(csv_dir, exist_ok=True)
        
        datos_procesados.to_csv(f"{csv_dir}/datos_procesados.csv", index=False)
        metrica_incidencia_7d.to_csv(f"{csv_dir}/incidencia_7d.csv", index=False)
        metrica_factor_crec_7d.to_csv(f"{csv_dir}/factor_crecimiento_7d.csv", index=False)
        
        context.add_output_metadata({
            'archivo_excel': dg.MetadataValue.path(output_path),
            'hojas_creadas': dg.MetadataValue.int(6),
            'directorio_csv': dg.MetadataValue.path(csv_dir),
            'archivos_generados': dg.MetadataValue.text("Excel + 3 CSVs individuales")
        })
        
        return output_path
        
    except Exception as e:
        context.log.error(f"Error generando reporte: {e}")
        raise

# TABLA DE PERFILADO INICIAL

@dg.asset(
    description="Genera tabla de perfilado básico de datos (para Paso 1 manual)",
    deps=[datos_procesados]
)
def tabla_perfilado(context: dg.AssetExecutionContext, datos_procesados: pd.DataFrame) -> pd.DataFrame:
    """
    Genera tabla de perfilado básico como se requiere en el Paso 1.
    """
    df = datos_procesados.copy()
    
    # Información básica
    perfilado = {
        'Métrica': [
            'Total de filas',
            'Total de columnas', 
            'Países únicos',
            'Rango de fechas',
            'Min new_cases',
            'Max new_cases',
            'Promedio new_cases',
            'Valores faltantes new_cases (%)',
            'Valores faltantes people_vaccinated (%)',
            'Población Ecuador (última)',
            f'Población {PAIS_COMPARATIVO} (última)'
        ],
        'Valor': [
            len(df),
            len(df.columns),
            df['location'].nunique(),
            f"{df['date'].min().date()} a {df['date'].max().date()}" if not df.empty else 'N/A',
            float(df['new_cases'].min()) if not df.empty else 'N/A',
            float(df['new_cases'].max()) if not df.empty else 'N/A',
            round(float(df['new_cases'].mean()), 2) if not df.empty else 'N/A',
            round(df['new_cases'].isnull().sum() / len(df) * 100, 2) if not df.empty else 0,
            round(df['people_vaccinated'].isnull().sum() / len(df) * 100, 2) if not df.empty else 0,
            float(df[df['location'] == 'Ecuador']['population'].iloc[-1]) if not df[df['location'] == 'Ecuador'].empty else 'N/A',
            float(df[df['location'] == PAIS_COMPARATIVO]['population'].iloc[-1]) if not df[df['location'] == PAIS_COMPARATIVO].empty else 'N/A'
        ]
    }
    
    df_perfilado = pd.DataFrame(perfilado)
    
    # Guardar como CSV
    output_dir = "/workspaces/proyecto-python/reportes"
    os.makedirs(output_dir, exist_ok=True)
    csv_path = f"{output_dir}/tabla_perfilado.csv"
    df_perfilado.to_csv(csv_path, index=False)
    
    context.add_output_metadata({
        'archivo_generado': dg.MetadataValue.path(csv_path),
        'metricas_calculadas': dg.MetadataValue.int(len(df_perfilado)),
        'preview': dg.MetadataValue.md(df_perfilado.to_markdown())
    })
    
    context.log.info(f"Tabla de perfilado guardada en: {csv_path}")
    
    return df_perfilado