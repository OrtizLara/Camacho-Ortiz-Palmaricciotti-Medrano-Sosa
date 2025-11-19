""""SEGUIMOS CON EL PUNTO 4."""
from abc import ABC
import pandas as pandas           
import peewee
from peewee import fn  
from modelo_orm import db, Comuna, Barrio, TipoObra, AreaResponsable, Empresa, Etapa, TipoContratacion, FuenteFinanciamiento, Obra   
from pathlib import Path

#Crear clase abstracta
class GestionarObra(ABC):
    CSV_PATH = Path(__file__).parent / "observatorio-de-obras-urbanas.csv"
    dataframe = None


#Punto A extraer datos!
    @classmethod
    def extraer_datos(cls):
        """
        leo el csv y devuelvo un DataFrame.
        * dtype=str para no pelearme con tipos raros
        * low_memory=False para evitar warnings que dsp nos molesten
        """
        """
        *intenta leer el CSV (bien)
        *si no existe → frena el programa a propósito
        *te da un mensaje claro diciendo: “Che, no encontré el CSV en tal ruta”"""
        try:
            df = pandas.read_csv(cls.CSV_PATH, dtype=str, low_memory=False, encoding='latin-1',sep=';')
            cls.dataframe = df
            print(f"(A) Extracción de datos del CSV '{cls.CSV_PATH}' exitosa.")
        except FileNotFoundError:
            raise FileNotFoundError(
                f"No encontre el csv en '{cls.CSV_PATH}'. "
                "Si el archivo tiene otro nombre o carpeta, cambiar GestionarObra.CSV_PATH."
            )
        except Exception as e:
            print(f"Error inesperado al leer el CSV: {e}")
            raise

    # A partir de aca tenemos la conexion y las tablas.


#Punto B conectar la base de datos!
    @classmethod
    def conectar_db(cls):
        """Abro la conexion; reuse_if_open=True evita error si ya estaba abierta."""
        try:
            db.connect(reuse_if_open=True)
            # Habilitar soporte de Foreign Keys en SQLite
            db.execute_sql('PRAGMA foreign_keys = ON;')
            print("(B) Conexión exitosa a la base de datos.")

        except peewee.OperationalError as e:
            print(f"Error al conectar con la base de datos: {e}")
            raise

#Punto C mapear el Orm Crear la estructura de tablas de la BD!
    @classmethod
    def mapear_orm(cls):
        """
        nos aseguramos de que las tablas existan.
        safe=True evita explotar si ya estaban creadas.
        """
        try:
            tablas = [Comuna, Barrio, TipoObra, AreaResponsable, Empresa, Etapa, TipoContratacion, FuenteFinanciamiento, Obra]
            db.create_tables(tablas, safe=True)
            print("(c) Mapeo ORM y creación de tablas exitosos.")
        except peewee.OperationalError as e:
            print(f"Error al crear las tablas: {e}")
            raise

    #Punto D A partir de aca hacemos la limpieza y la normalizacion
    @classmethod
    def limpiar_datos(cls):
        #limpiar_datos(), que debe incluir las sentencias necesarias para realizar la “limpieza” de 
        #los datos nulos y no accesibles del Dataframe.

        if cls.dataframe is None:
            print("Error: No hay dataframe para limpiar. Ejecute extraer_datos() primero.")
            return
        
        df= cls.dataframe.copy()
        """
        limpio y normalizo columnas del csv
        uso esto para
          * homogeneizar nombres de columnas (minusculas)
          * mapear a nuestras claves internas
          * dejar strings prolijos
          * tipar numeros o fechas sin romper
          * generar una especie de "codigo" único si falta
        """
        
        df = df.rename(columns=str.lower)

        # mapping flexible csv -> nombres internos (si el dataset cambia, vamos a tener que tocar aca ) 
        #No se que hace esto, normaliza columnas? crea una matriz?
        colmap = {
            'nombre': ['nombre', 'obra', 'nombre_obra'],
            'barrio': ['barrio'],
            'empresa': ['empresa', 'contratista', 'licitacion_oferta_empresa'],
            'tipo_obra': ['tipo_obra', 'tipo'],
            'etapa': ['etapa'],
            'monto_contrato': ['monto', 'monto_contrato'],
            'fecha_inicio': ['fecha_inicio'],
            'fecha_fin_inicial': ['fecha_fin_inicial', 'fecha_fin'],
            'lat': ['lat', 'latitude'],
            'lng': ['lon', 'long', 'longitude', 'lng'],
            'comuna': ['comuna'],
            'area_responsable': ['area_responsable'],
            'tipo_contratacion': ['tipo_contratacion', 'contratacion_tipo'],
            'fuente_financiamiento': ['fuente_financiamiento', 'financiamiento_fuente', 'financiamiento'],
            'descripcion': ['descripcion'],
            'entorno': ['entorno'],
            'direccion': ['direccion'],
            'plazo_meses': ['plazo_meses'],
            'porcentaje_avance': ['porcentaje_avance'],
            'mano_obra': ['mano_obra'],
            'nro_contratacion': ['nro_contratacion'],
            'nro_expediente': ['nro_expediente'],
            'cuit_contratista': ['cuit_contratista'],
            'destacada': ['destacada']
        }

        
        # Invertir el mapa para poder renombrar
        rename_map = {}
        keep_cols = []
        for internal_name, csv_names in colmap.items():
            for csv_name in csv_names:
                if csv_name in df.columns:
                    rename_map[csv_name] = internal_name
                    if csv_name not in keep_cols: # Evitar duplicados si el nombre ya es correcto
                        keep_cols.append(csv_name)
                    break # Encontramos una, pasamos al siguiente nombre interno
    
        df = df[keep_cols].rename(columns=rename_map)

        #Reemplazar NaNs de Pandas por None de Python 
        df = df.where(pandas.notnull(df), None)

        #Limpiar y estandarizar columnas de texto (FKs)
        columnas_texto_fk = [
            'etapa', 'tipo_obra', 'area_responsable', 'barrio', 
            'empresa', 'tipo_contratacion', 'fuente_financiamiento'
        ]
        
        for col in columnas_texto_fk:
            if col in df.columns:
                es_nulo = df[col].isnull()
                # .str.title() es clave para unificar "Palermo" y "palermo"
                df[col] = (
                    df[col].astype(str).str.strip().str.title().replace("None", None) 
                    # Estandariza "en ejecución" -> "En Ejecución"
                    # # Reemplaza el texto "None" si existe
                )
                if col == 'barrio':
                    df[col] = df[col].str.replace("Monserrat", "Montserrat", case=False)
                df[col] = df[col].str.normalize('NFD').str.encode('ascii', 'ignore').str.decode('utf-8')
                df[col] = df[col].str.replace("Secretari A", "Secretaria", case=False)
                df[col] = df[col].str.replace(r'\s+', ' ', regex=True)
                df[col] = df[col].str.strip()
                df[col] = df[col].replace("None", None)
                df.loc[es_nulo, col] = None # Restaurar nulos


        # 4. Limpiar y tipar números y fechas
        for c in ["monto_contrato", "lat", "lng", "porcentaje_avance", "plazo_meses", "mano_obra"]:
            if c in df.columns:
                # Quitar símbolos de $ antes de convertir
                if c == 'monto_contrato':
                    df[c] = (df[c].astype(str).str.replace(r'[$,]', '', regex=True))  # Quita $ y ,
                # errors='coerce' convierte errores en NaT/NaN (que luego son None)
                df[c] = pandas.to_numeric(df[c], errors="coerce")

        for d in ["fecha_inicio", "fecha_fin_inicial"]:
            if d in df.columns:
                # errors='coerce' convierte fechas inválidas en NaT (Not a Time)
                df[d] = pandas.to_datetime(df[d], dayfirst=True, errors="coerce").dt.date

        df["codigo"] = (
            df["nombre"].fillna("SIN_NOMBRE").str.upper() + "-" +
            df["barrio"].fillna("SIN_BARRIO").str.upper()
        )
        
        df = df.drop_duplicates(subset=["codigo"], keep='first')

        print("(D) Limpieza de datos completada.")
        cls.dataframe=df

    # Punto E cargar datos! ORM
    """
    e. cargar_datos(), que debe incluir las sentencias necesarias para persistir los datos de las 
    obras (ya transformados y “limpios”) que contiene el objeto Dataframe en la base de  
    datos relacional SQLite. Para ello se debe utilizar el método de clase Model create() en  
    cada una de las clase del modelo ORM definido.  
    """
    @classmethod
    def cargar_datos(cls):

        """
        (e) Persiste los datos del DataFrame (cls.dataframe) en la BD SQLite.
        
        1. Carga las "Tablas Catálogo" (Barrio, TipoObra, etc.) usando get_or_create().
        2. Carga la tabla principal "Obra" usando create() y los FKs.
        usar Model.create().
        """

        if cls.dataframe is None:
            print("No hay DataFrame para cargar.")
            return

        print("Iniciando carga de datos.")

        try:
            # nos aAsegurarse de que las tablas existan
            cls.mapear_orm() 

            # Cargar Tablas Catálogo (FKs) ---
            # Usamos "caches" (diccionarios) para no consultar la BD miles de veces, sino solo una vez por valor único.

            comunas_df = cls.dataframe[['comuna', 'barrio']].drop_duplicates()
            comunas_cache = {}
            barrios_cache = {}
            
            with db.atomic(): # Transacción para las comunas/barrios
                for _, row in comunas_df.iterrows():
                    comuna_num = str(row['comuna']) if row['comuna'] else "Sin Comuna"
                    barrio_nom = str(row['barrio']) if row['barrio'] else "Sin Barrio"
                    
                    # Get-or-Create para Comuna
                    if comuna_num not in comunas_cache:
                        comuna_obj, _ = Comuna.get_or_create(numero=comuna_num)
                        comunas_cache[comuna_num] = comuna_obj
                    else:
                        comuna_obj = comunas_cache[comuna_num]
                    
                    # Get-or-Create para Barrio
                    if barrio_nom not in barrios_cache:
                        barrio_obj, _ = Barrio.get_or_create(nombre=barrio_nom, comuna=comuna_obj)
                        barrios_cache[barrio_nom] = barrio_obj

            # Función helper para cargar catálogos simples
            def cargar_catalogo_cache(columna_df, Modelo):
                cache_dict = {}
                items_unicos = cls.dataframe[columna_df].dropna().unique()
                with db.atomic():
                    for item in items_unicos:
                        obj, _ = Modelo.get_or_create(nombre=item)
                        cache_dict[item] = obj
                return cache_dict

            # Creamos todas las FKs
            tipos_obra_cache = cargar_catalogo_cache('tipo_obra', TipoObra)
            areas_cache = cargar_catalogo_cache('area_responsable', AreaResponsable)
            empresas_cache = cargar_catalogo_cache('empresa', Empresa)
            etapas_cache = cargar_catalogo_cache('etapa', Etapa)
            tipos_contrat_cache = cargar_catalogo_cache('tipo_contratacion', TipoContratacion)
            fuentes_finan_cache = cargar_catalogo_cache('fuente_financiamiento', FuenteFinanciamiento)
            
            # Cargar Tabla Principal "Obra"
            # Ahora iteramos el DataFrame y usamos create() 
            
            # El DF pasa a ser una lista de diccionarios para iterar
            filas_obras = cls.dataframe.to_dict('records')
            
            with db.atomic(): # Transacción masiva para todas las obras
                for row in filas_obras:
                    
                    # Buscamos los OBJETOS FK en nuestros caches
                    barrio_obj = barrios_cache.get(row['barrio'])
                    tipo_obra_obj = tipos_obra_cache.get(row['tipo_obra'])
                    area_obj = areas_cache.get(row['area_responsable'])
                    empresa_obj = empresas_cache.get(row['empresa'])
                    etapa_obj = etapas_cache.get(row['etapa'])
                    tipo_contrat_obj = tipos_contrat_cache.get(row['tipo_contratacion'])
                    fuente_finan_obj = fuentes_finan_cache.get(row['fuente_financiamiento'])
                    pa_valor = row.get('porcentaje_avance')
                    pa_final = 0 if pandas.isna(pa_valor) else pa_valor
                    f_inicio_valor = row.get('fecha_inicio')
                    f_inicio_final = None if pandas.isna(f_inicio_valor) else f_inicio_valor

                    f_fin_valor = row.get('fecha_fin_inicial')
                    f_fin_final = None if pandas.isna(f_fin_valor) else f_fin_valor

                    # Usamos el método Model.create() como es pedido
                    Obra.create(

                        # Campos directos 
                        nombre=row.get('nombre'),
                        descripcion=row.get('descripcion'),
                        entorno=row.get('entorno'),
                        monto_contrato=row.get('monto_contrato'),
                        direccion=row.get('direccion'),
                        lat=row.get('lat'),
                        lng=row.get('lng'),
                        fecha_inicio=f_inicio_final,
                        fecha_fin_inicial=f_fin_final,
                        plazo_meses=row.get('plazo_meses'),
                        porcentaje_avance=pa_final,
                        mano_obra=row.get('mano_obra'),
                        nro_contratacion=row.get('nro_contratacion'),
                        nro_expediente=row.get('nro_expediente'),
                        cuit_contratista=row.get('cuit_contratista'),
                        destacada=row.get('destacada'),
                        
                        # Campos ForeignKey
                        barrio=barrio_obj,
                        tipo_obra=tipo_obra_obj,
                        area_responsable=area_obj,
                        empresa=empresa_obj,
                        etapa=etapa_obj,
                        tipo_contratacion=tipo_contrat_obj,
                        fuente_financiamiento=fuente_finan_obj
                    )
            
            print(f"Carga de {len(filas_obras)} obras completada.")
            print(f"(E) Carga de datos finalizada exitosamente.")

        except peewee.IntegrityError as e:
            print(f"Error de integridad durante la carga de datos: {e}")
            db.rollback()
            raise e
        except Exception as e:
            print(f"Error inesperado durante la carga de datos: {e}")
            db.rollback()
            raise


#Helper para buscar y validar un Foreign Key por teclado.
    @classmethod
    def _buscar_fk(cls, Modelo, campo_busqueda='nombre'):
        """
        Helper robusto para buscar Foreign Keys.
        1. Intenta búsqueda EXACTA (case-insensitive).
        2. Si falla, intenta búsqueda PARCIAL (ilike).
        3. Maneja ambigüedad.
        """
        while True:
            valor_ingresado = input(f"  Ingrese {Modelo.__name__} (buscar por {campo_busqueda}): ")
            if not valor_ingresado:
                print("  La entrada no puede estar vacía.")
                continue
            
            try:
                # --- NIVEL 1: BÚSQUEDA EXACTA ---
                # Priorizamos si el usuario escribió el nombre completo
                try:
                    # .ilike(valor) SIN comodines % busca igualdad ignorando mayúsculas
                    coincidencia_exacta = Modelo.get(getattr(Modelo, campo_busqueda).ilike(valor_ingresado))
                    print(f"  ✓ Encontrado: {getattr(coincidencia_exacta, campo_busqueda)}")
                    return coincidencia_exacta
                except peewee.DoesNotExist:
              
                    pass
                
                # --- NIVEL 2: BÚSQUEDA PARCIAL (CONTAINS) ---
                condicion = getattr(Modelo, campo_busqueda).ilike(f'%{valor_ingresado}%')
                query = Modelo.select().where(condicion)
                cantidad = query.count()

                if cantidad == 1:

                    instancia = query.get()
                    print(f"  ✓ Encontrado (parcial): {getattr(instancia, campo_busqueda)}")
                    return instancia

                elif cantidad > 1:

                    print(f" La búsqueda '{valor_ingresado}' es ambigua ({cantidad} coincidencias).")
                    print("  Por favor sea más específico. Ejemplos encontrados:")
                    for item in query.limit(5):
                        print(f"   * {getattr(item, campo_busqueda)}")
                    # Vuelve al inicio del while para pedir input de nuevo
                    continue

                else:
                    # Ninguno coincide
                    print(f"  No se encontró nada similar a '{valor_ingresado}'.")
                    if input("    ¿Desea ver una lista de opciones? (s/n): ").lower() == 's':
                        print(f"    --- Lista de {Modelo.__name__} ---")
                        for item in Modelo.select().limit(10): 
                            print(f"    - {getattr(item, campo_busqueda)}")

            except Exception as e:
                print(f"  ✗ Error inesperado en la búsqueda: {e}")
                return None


#F Crea una nueva instancia de Obra desde la terminal.
    @classmethod
    def nueva_obra(cls):
        print("\n--- (f) Creación de Nueva Obra ---")
        try:
            # 1. Pedir nombre
            nombre_obra = input("Ingrese el nombre de la nueva obra: ")
            
            # 2. Buscar FKs existentes (Punto 8)
            print("Buscando Tipo de Obra...")
            tipo_obra_fk = cls._buscar_fk(TipoObra)
            
            print("Buscando Área Responsable...")
            area_fk = cls._buscar_fk(AreaResponsable)
            
            print("Buscando Barrio...")
            barrio_fk = cls._buscar_fk(Barrio)
            
            # 3. Crear la obra con los valores (usando Model.create())
            nueva_obra_obj = Obra.create(
                nombre=nombre_obra,
                tipo_obra=tipo_obra_fk,
                area_responsable=area_fk,
                barrio=barrio_fk
            )
            
            # 4. Llamar a nuevo_proyecto() SIN parámetros
            nueva_obra_obj.nuevo_proyecto()
            
            print(f"Obra '{nombre_obra}' creada con ID: {nueva_obra_obj.id}")
            return nueva_obra_obj
            
        except Exception as e:
            print(f"Error: {e}")
            return None


#G Obtiene y muestra indicadores de la base de datos.
    @classmethod
    def obtener_indicadores(cls):
        try:
            #a. Listado de todas las áreas responsables 
            print("\nÁreas Responsables:")
            for area in AreaResponsable.select():
                print(f"  - {area.nombre}")

            # b.Listado de todos los tipos de obra 
            print("\nTipos de Obra:")
            for tipo in TipoObra.select():
                print(f"  - {tipo.nombre}")

            # c. Cantidad de obras que se encuentran en cada etapa
            print("\nObras por Etapa:")
            query_c = (
                Etapa.select(Etapa.nombre, peewee.fn.COUNT(Obra.id).alias('cantidad'))
                .join(Obra, peewee.JOIN.LEFT_OUTER)
                .group_by(Etapa.nombre)
                .order_by(peewee.fn.COUNT(Obra.id).desc())
            )
            for etapa in query_c:
                print(f"  - {etapa.nombre}: {etapa.cantidad} obras")

            # d. Cantidad de obras y monto total de inversión por tipo de obra 
            print("\nInversión por Tipo de Obra:")
            query_d = (
                TipoObra.select(
                    TipoObra.nombre,
                    peewee.fn.COUNT(Obra.id).alias('cantidad'),
                    peewee.fn.SUM(Obra.monto_contrato).alias('total')
                )
                .join(Obra, peewee.JOIN.LEFT_OUTER)
                .group_by(TipoObra.nombre)
                .order_by(peewee.fn.SUM(Obra.monto_contrato).desc())
            )
            for tipo in query_d:
                monto = tipo.total if tipo.total else 0
                print(f"  - {tipo.nombre}: {tipo.cantidad} obras - Total: ${monto:,.2f}")

            # e. Listado de todos los barrios pertenecientes a las comunas 1, 2 y 3
            print("\nBarrios en Comunas 1, 2 y 3:")
            query_e = (
                Barrio.select(Barrio.nombre, Comuna.numero)
                .join(Comuna)
                .where(Comuna.numero.in_(['1', '2', '3']))
                .order_by(Comuna.numero, Barrio.nombre)
            )
            for barrio in query_e:
                print(f"  - Comuna {barrio.comuna.numero}: {barrio.nombre}")

            # f. Cantidad de obras finalizadas en un plazo menor o igual a 24 meses 
            print("\nObras finalizadas en 24 meses o menos:")
            etapa_finalizada = Etapa.get_or_none(Etapa.nombre == "Finalizada")
            if etapa_finalizada:
                query_f = Obra.select().where(
                    (Obra.etapa == etapa_finalizada) &
                    (Obra.plazo_meses <= 24)
                )
                print(f"  - {query_f.count()} obras.")
            else:
                print("  - No se encontró la etapa 'Finalizada'.")

            # g. Monto total de inversión
            print("\n[17.g] Monto Total de Inversión (Todas las obras):")
            query_g = Obra.select(peewee.fn.SUM(Obra.monto_contrato).alias('total_general'))
            total_g = query_g.scalar() or 0
            print(f"  - ${total_g:,.2f}")

        except peewee.OperationalError as e:
            print(f"Error al ejecutar las consultas de indicadores: {e}")
        except Exception as e:
            print(f"Error inesperado al obtener indicadores: {e}")
